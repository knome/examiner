
# the universal data accessor
#   guaranteed to access any and all data or your money back!*
#   
# * no money will actually be given back

# Information Sources
#   http://wiki.osdev.org/ISO_9660
#   http://newosxbook.com/DMG.html
#       found after noticing the term "koly" when running 'strings' against a test img
#   http://en.wikipedia.org/wiki/Apple_Partition_Map#Layout
#   http://dubeiko.com/development/FileSystems/HFSPLUS/tn1150.html
#   https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout
#   https://people.gnome.org/~markmc/qcow-image-format.html
#   https://code.google.com/p/theunarchiver/wiki/StuffItFormat
#   https://code.google.com/p/theunarchiver/wiki/StuffIt5Format

# Thanks.


import struct
import uuid
import re
import base64
import zlib
import traceback


####################################################################################
## global to control block memoization
## every memoized block function must have an entry here or it will die

# name : number-of-returns-to-memoize
MEMOIZATION = {
    'file-blocks'               : 10 ,
    'qcow2-blocks'              : 10 ,
    'ext-inode-contents-blocks' : 10 ,
    'apple-disk-image-blocks'   : 10 ,
    }

# for the File__BlockDevice class
FILE_BLOCK_SIZE = 4096

####################################################################################
## simple print statement debuggers to active / deactivate during development

class Debugger():
    def __init__( self, name, active ):
        self._name = name
        self._active = active
        return
    
    def debug( self, * messages ):
        if self._active:
            print ( '[%s]' % self._name ) ,
            for message in messages:
                print message ,
            print
        return
    
    __call__ = debug

DEBUG_FILE    = Debugger( 'DEBUG-FILE'   , False )
DEBUG_EXT     = Debugger( 'DEBUG-EXT'    , False )
DEBUG_MEMO    = Debugger( 'DEBUG-MEMO'   , False )
DEBUG_BACKING = Debugger( 'DEBUG-BACKING', False )

####################################################################################
## the cursor makes for simple interaction with the rados

class Cursor():
    def __init__( self, name, rado, position = 0 ):
        if position < 0: raise Exception( 'recieved negative position' )
        
        self._name     = name
        self._rado     = rado
        self._position = position
        return
    
    def __repr__( self ):
        return '<Cursor name:%s position:%s rado:%s>' % (
            repr( self._name     ) ,
            repr( self._position ) ,
            repr( self._rado     ) ,
            )
    
    def rado( self, size = None ):
        # create a rado of size from the current location
        # do not optimize a size None rado from offset 0 to return the underlying rado directly
        # some rados depend on RadoRado to massage incoming reads, and may fail if it is bypassed
        return RadoRado( 
            name   = 'cursor-rado'  ,
            rado   = self._rado     ,
            offset = self._position ,
            size   = size           ,
            )
    
    def readlen( self, amount ):
        if amount < 0: raise Exception( 'recieved negative amount' )
        
        chunk, chunklen = rv = self._rado.readatlen( self._position, amount )
        self._position += chunklen
        return rv
    
    def read( self, amount ):
        chunk, _ = self.readlen( amount )
        return chunk
    
    def skip( self, amount ):
        # you may skip backwards, but never before position 0
        
        self._position += amount
        if self._position < 0: raise Exception( 'skipped position to negative value' )
        
        return
    
    def seek( self, position ):
        if position < 0: raise Exception( 'received negative position' )
        
        self._position = position
        return
    
    def tell( self ):
        return self._position
    
    def end( self ):
        self._position = self._rado.size()
    
    # helper functions
    
    def clipped( self, amount ):
        # reads amount, returns only portion that would
        # qualify as a well formed string in C. cuts off
        # everything past the first \0
        return self.read( amount ).split( '\x00' )[0]
    
    def readall( self, amount ):
        chunk, chunklen = self.readlen( amount )
        if chunklen != amount:
            raise Exception( 'expected %s bytes, found %s' % (
                    repr( amount   ) ,
                    repr( chunklen ) ,
                    ))
        return chunk
    
    def uuid( self ):
        return str( uuid.UUID( bytes = self.read( 16 ) ) )
    
    def uint64lsb( self ):
        return struct.unpack( '<Q', self.read( 8 ) )[ 0 ]
    
    def uint64msb( self ):
        return struct.unpack( '>Q', self.read( 8 ) )[ 0 ]
    
    def uint32lsb( self ):
        return struct.unpack( '<I', self.read( 4 ) )[ 0 ]
    
    def uint32msb( self ):
        return struct.unpack( '>I', self.read( 4 ) )[ 0 ]
    
    def uint16lsb( self ):
        return struct.unpack( '<H', self.read( 2 ) )[ 0 ]
    
    def uint16msb( self ):
        return struct.unpack( '>H', self.read( 2 ) )[ 0 ]
    
    def uint8( self ):
        return struct.unpack( 'B', self.read( 1 ) )[ 0 ]
    
    def sint8( self ):
        return struct.unpack( 'b', self.read( 1 ) )[ 0 ]
    
    # helperer functions
    
    def readline( self ):
        line = []
        
        while True:
            b = self.read( 1 )
            line.append( b )
            if b in [ '\n', '' ]:
                return ''.join( line )
    
    def readlines( self ):
        while True:
            b = self.read( 1 )
            if b == '':
                return
            else:
                self.skip( -1 )
                yield self.readline()



#########################################################################################
## helpers

class Common():
    
    @staticmethod
    def required_reads( blockSize, position, amount ):
        # return a list describing the reads required to gather the data specified by
        # the given position and amount while never crossing the boundaries between blocks
        # useful for reading from any block storage format
        
        reads         = []
        currentSector = position / blockSize
        
        if position % blockSize != 0:
            initialReadAmount = min( blockSize - position % blockSize ,
                                     amount                           ,
                                     )
            reads.append({
                    'sector' : currentSector        ,
                    'offset' : position % blockSize ,
                    'amount' : initialReadAmount    ,
                    })
            currentSector += 1
            amount -= initialReadAmount
        
        while amount:
            readAmount = min( blockSize ,
                              amount    ,
                              )
            reads.append({
                    'sector' : currentSector ,
                    'offset' : 0             ,
                    'amount' : readAmount    ,
                    })
            currentSector += 1
            amount -= readAmount
        
        return reads
    
    @staticmethod
    def nicely( v ):
        return format( v, '#064b' )
    
    @staticmethod
    def memoize( name ):
        if name not in MEMOIZATION:
            raise Exception( 'you must specify the number of results to memoize for %s' % repr( name ) )
        else:
            def decorate( fn ):
                
                if MEMOIZATION[ name ] == 0:
                    DEBUG_MEMO( "don't bother memoizing if you can't actually store anything" )
                    return fn
                
                memoized = {}
                lastUsed = []
                
                def wrapped( *args, **kwargs ):
                    key = args + tuple( sorted( kwargs.items() ) )
                    
                    if key in memoized:
                        DEBUG_MEMO( 'USE MEMOIZED VALUE' )
                        lastUsed.remove( key )
                        lastUsed.append( key )
                        return memoized[ key ]
                    else:
                        DEBUG_MEMO( 'ADD MEMOIZED VALUE' )
                        rv = fn( *args, **kwargs )
                        memoized[ key ] = rv
                        lastUsed.append( key )
                        if len( lastUsed ) > MEMOIZATION[ name ]:
                            DEBUG_MEMO( 'REM MEMOIZED VALUE' )
                            del memoized[ lastUsed.pop( 0 ) ]
                        return rv
                return wrapped
            return decorate


####################################################################################
## random access data objects, nestable to extract data in complex patterns at will

class RadoBlob():
    # random access data object for string data blob
    
    def __init__( self, name, blob ):
        self._name = name
        self._blob = blob
        self._size = len( blob )
        return
    
    def __repr__( self ):
        return '<RadoBlob name:%s blob:%s>' % (
            repr( self._name ) ,
            repr( self._blob ) ,
            )
    
    def cursor( self ): return Cursor(
        name = 'radoblob-cursor',
        rado = self             ,
        )
    
    def readatlen( self, position, amount ):
        if position < 0: raise Exception( 'recieved negative position' )
        if amount   < 0: raise Exception( 'recieved negative amount'   )
        
        v = self._blob[ position : position + amount ]
        
        # print 'READ %s:%s FROM %s YIELDING:%s' % (
        #     repr( position   ) ,
        #     repr( amount     ) ,
        #     repr( self._size ) ,
        #     repr( v          ) ,
        #     )
        
        return v, len( v )



class RadoFile():
    
    def __init__( self, name, fileobj ):
        self._name = name
        self._fo   = fileobj
        
        self._fo.seek( 0, 2 )
        self._size = self._fo.tell()
        return
    
    def __repr__( self ):
        return '<RadoFile name:%s fileobj:%s>' % (
            repr( self._name ) ,
            repr( self._fo   ) ,
            )
    
    def cursor( self ):
        return Cursor(
            name = 'radofile-cursor' ,
            rado = self              ,
            )
    
    def size( self ):
        return self._size
    
    def readatlen( self, position, amount ):
        
        DEBUG_FILE( 'FILE[:position=%s:amount=%s]' % (
                str( position ),
                str( amount   ),
                ))
        
        if position < 0: raise Exception( 'recieved negative position' )
        if amount   < 0: raise Exception( 'recieved negative amount'   )
        
        self._fo.seek( position )
        chunk = self._fo.read( amount )
        
        return chunk, len( chunk )


class File__BlockDevice():
    def __init__( self, name, fileobj ):
        self._name    = name
        self._fileobj = fileobj
        
        # sneaky seek to end to get filesize
        self._fileobj.seek( 0, 2 )
        self._size = self._fileobj.tell()
        return
    
    def __repr__( self ):
        return '<File__BlockDevice name:%s fileobj:%s>' % (
            repr( self._name    ) ,
            repr( self._fileobj ) ,
            )
    
    def block_size( self ):
        return FILE_BLOCK_SIZE
    
    def size( self ):
        return self._size
    
    @Common.memoize( 'file-blocks' )
    def get_block( self, blockNo ):
        
        DEBUG_FILE( 'FILE[:position=%s:amount=%s]' % (
                str( blockNo * FILE_BLOCK_SIZE ),
                str( FILE_BLOCK_SIZE           ),
                ))
        
        self._fileobj.seek( blockNo * FILE_BLOCK_SIZE )
        return RadoBlob(
            name = 'file-block-device/get-block-rado' ,
            blob = self._fileobj.read( FILE_BLOCK_SIZE )
            )


class RadoRado():
    # anything requiring a specific segment of another rado should be run through this
    # as the read capping logic exists only here for simplicity in other rados
    
    def __init__( self, name, rado, offset = 0, size = None ):
        self._name   = name
        self._rado   = rado
        self._offset = offset
        
        self._size   = (
            size if size != None else ( rado.size() - self._offset )
            )
        
        return
    
    def __repr__( self ):
        return '<RadoRado name:%s offset:%s size:%s rado:%s>' % (
            repr( self._name   ) ,
            repr( self._offset ) ,
            repr( self._size   ) ,
            repr( self._rado   ) ,
            )
    
    def cursor( self ):
        return Cursor(
            name = 'radorado-cursor' ,
            rado = self              ,
            )
    
    def size( self ):
        return self._size
    
    def readatlen( self, position, amount ):
        if position < 0: raise Exception( 'received negative position' )
        if amount   < 0: raise Exception( 'received negative amount'   )
        
        if self._size == None:
            raise Exception( 'oh god wat' )
        
        else:
            # size underflow detection?
            # like, the size thinks there should be data, but the underlying rado runs out?
            
            if position > self._size:
                return '', 0
            
            if position + amount > self._size:
                amount = self._size - position
            
            return self._rado.readatlen( self._offset + position, amount )


class RadoBlock():
    # rado over a block device
    # calls .size()
    # calls .block_size()
    # calls .get_block( blockNo )
    
    def __init__( self, name, blockDevice ):
        self._name        = name
        self._blockDevice = blockDevice
        self._blockSize   = blockDevice.block_size()
        self._size        = blockDevice.size()
        return
    
    def __repr__( self ):
        return '<RadoBlock name:%s blocksize:%s size:%s blockDevice:%s>' % (
            repr( self._name        ) ,
            repr( self._blockSize   ) ,
            repr( self._size        ) ,
            repr( self._blockDevice ) ,
            )
    
    def cursor( self ):
        # use RadoRado to clip and check incoming readatlen args
        # 
        return Cursor(
            name = 'radoblock-indirect-cursor' ,
            rado = self                        ,
            ).rado().cursor()
    
    def size( self ):
        # not in blocks. final block may be only partially filled with
        # usable data. we won't read past where we're told is good
        # 
        return self._blockDevice.size()
    
    def readatlen( self, position, amount ):
        # position and amount have passed through RadoRado, so they should
        # never ask for data that the underlying block device does not 
        # purport to have
        # 
        
        requiredReads = Common.required_reads(
            blockSize = self._blockSize ,
            position  = position        ,
            amount    = amount          ,
            )
        
        bits = []
        for requiredRead in requiredReads:
            blockDataRado = self._blockDevice.get_block( requiredRead['sector'] )
            cursor = blockDataRado.cursor()
            cursor.seek( requiredRead['offset'] )
            bits.append( cursor.read( requiredRead['amount'] ) )
        
        data = ''.join( bits )
        return data, len( data )


class RadoZero():
    # rado providing a range of zeroed memory
    
    def __init__( self, name, size ):
        self._name = name
        self._size = size
        return
    
    def __repr__( self ):
        return '<RadoZero name:%s size:%s>' % (
            repr( self._name ) ,
            repr( self._size ) ,
            )
    
    def cursor( self ):
        return Cursor( 
            name = 'radozero-indirect-cursor' ,
            rado = self                       ,
            ).rado().cursor()
    
    def size( self ):
        return self._size
    
    def readatlen( self, position, amount ):
        return '\0', amount


###############################################################################################
## attributes handler

class Attributes():
    # an ordered dict with custom string-ing
    
    class Default(): pass
    
    @staticmethod
    def from_iterator( iterator ):
        attributes = Attributes()
        for item in iterator:
            attributes.append( item )
        return attributes
    
    def __init__( self ):
        self._byname  = {}
        self._byorder = []
        
        self._longestName = 0
        return
    
    def contains( self, name ):
        return name in self._byname
    
    def put( self, name, value ):
        if name in self._byname:
            self._byname[ name ][ 0 ] = value
        else:
            box = [ value ]
            self._byname[ name ] = box
            self._byorder.append( (name, box) )
            if len( name ) > self._longestName:
                self._longestName = len( name )
    
    def append( self, value ):
        # will add one to last key ( assumed number ) stored and append
        # useful for building numeric array, not for anything else really
        
        ll = int( self._byorder[-1][0] ) if self._byorder else 0
        nn = 1 + ll
        self.put( str( nn ), value )
        return
    
    def get( self, name, default = Default ):
        if name in self._byname:
            return self._byname[ name ][ 0 ]
        elif default != self.Default:
            return default
        else:
            raise Exception(
                'no such attribute %s' % repr( name )
                )
        
    def to_string( self, depth = 0 ):
        out = []
        
        out.append( ( " " * depth ) + "{\n" )
        for ( name, box ) in self._byorder:
            out.append(
                "  "
                + ( depth * " " )
                + name
                + ( " " * ( self._longestName - len( name ) ) )
                + " : " 
                + ( "\n" + box[ 0 ].to_string( depth = depth + 2 ) 
                    if isinstance( box[ 0 ], Attributes ) 
                    else repr( box[ 0 ] )
                    )
                + "\n"
                )
        out.append( ( " " * depth ) + "}" )
        
        return ''.join( out )
    
    def to_flat( self ):
        return ' ; '.join( re.sub( ' +', ' ', self.to_string() ).split( '\n' ) )
    
    def items( self ):
        return [ (k, v[0]) for k,v in self._byorder ]
    
    def __str__( self ):
        return self.to_string()
    
    def __repr__( self ):
        return self.to_string()


#########################################################################################
## models

MODELS         = []
MODELS_BY_NAME = {}

def model( target ):
    MODELS.append( target )
    MODELS_BY_NAME[ target.name ] = target
    return target

def determine_compatible_models( rado ):
    for model in MODELS:
        try:
            if model.matches( rado ):
                yield True, model
            else:
                yield False, model
        except Exception, e:
            print '# exception checking %s' % repr( model )
            traceback.print_exc()
            yield False, model

def first_compatible_model( rado ):
    for compatible, model in determine_compatible_models( rado ):
        if compatible:
            return model
    return None

def model_by_name( name ):
    if name not in MODELS_BY_NAME:
        raise Exception( 'unknown model : %s' % repr( name ) )
    else:
        return MODELS_BY_NAME[ name ]


class ModelUnknownBlob():
    # generic unknown whatever, used as initial model
    name = 'unknown-blob'
    
    def __init__( self, rado ):
        self._rado = rado
        return
    
    def is_listable( self ): return False
    def is_radoable( self ): return True
    
    def rado( self ):
        return self._rado


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- CDFS ( cdrom file system / .iso file )

@model
class FileSystem__CompactDiskFileSystem():
    name = 'file-system--compact-disk-file-system'
    
    _SYSTEM_AREA_SIZE       = 32768
    _VOLUME_DESCRIPTOR_SIZE = 2048
    
    @staticmethod
    def matches( rado ):
        cursor = rado.cursor()
        
        cursor.skip( FileSystem__CompactDiskFileSystem._SYSTEM_AREA_SIZE )
        cursor.skip( 1 ) # type byte size
        
        magicNumber = cursor.read( 5 )
        
        return magicNumber == 'CD001'
    
    def __init__( self, rado ):
        self._rado = rado
        return
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    def list( self ):
        return [
            ('iso'       , 'root directory of the iso filesystem'        ),
            ('rock-ridge', 'root directory of the rock-ridge filesystem' ),
            ('juliet'    , 'root directory of the juliet filesystem'     ),
            ]
    
    def _volume_descriptor_rados( self ):
        volumeDescriptorOffset = self._SYSTEM_AREA_SIZE
        while True:
            volumeDescriptorRado = RadoRado(
                rado   = self._rado                   ,
                offset = volumeDescriptorOffset       ,
                size   = self._VOLUME_DESCRIPTOR_SIZE ,
                )
            
            volumeDescriptorCursor = volumeDescriptorRado.cursor()
            
            volumeType = volumeDescriptorCursor.read( 1 )
            if volumeType == '\xFF':
                return
            else:
                yield (volumeType, volumeDescriptorRado)
            
            volumeDescriptorOffset += self._VOLUME_DESCRIPTOR_SIZE
    
    def _volume_information( self ):
        volumeInformation = Attributes()
        
        volumeInformation.put( 'boot-record-volume-descriptor'   , 'not-present' )
        volumeInformation.put( 'primary-volume-descriptor'       , 'not-present' )
        volumeInformation.put( 'supplementary-volume-descriptor' , 'not-present' )
        volumeInformation.put( 'volume-partition-descriptor'     , 'not-present' )
        
        for ( volumeType, volumeRado ) in self._volume_descriptor_rados():
            
            if volumeType == '\x00':
                bootAttributes = Attributes()
                cursor         = volumeRado.cursor()
                
                bootAttributes.put( 'boot-version'          , cursor.read( 1 )     )
                bootAttributes.put( 'boot-system-identifier', cursor.clipped( 32 ) )
                bootAttributes.put( 'boot-identifier'       , cursor.clipped( 32 ) )
                
                volumeInformation.put( 'boot-record-volume-descriptor', bootAttributes )
                continue
            
            if volumeType == '\x01':
                primaryAttributes = Attributes()
                cursor            = volumeRado.cursor()
                
                cursor.seek(  8 )
                primaryAttributes.put( 'system-identifier', cursor.clipped( 32 ) )
                primaryAttributes.put( 'volume-identifier', cursor.clipped( 32 ) )
                
                cursor.seek( 80 )
                primaryAttributes.put( 'volume-space-size', cursor.uint32lsb() )
                
                cursor.seek( 120 )
                primaryAttributes.put( 'volume-set-size', cursor.uint16lsb() )
                
                cursor.seek( 124 )
                primaryAttributes.put( 'volume-sequence-number', cursor.uint16lsb() )
                
                cursor.seek( 128 )
                primaryAttributes.put( 'logical-block-size', cursor.uint16lsb() )
                
                cursor.seek( 132 )
                primaryAttributes.put( 'path-table-size', cursor.uint32lsb() )
                
                cursor.seek( 140 )
                primaryAttributes.put( 'path-table-lba-location'         , cursor.uint32lsb() )
                primaryAttributes.put( 'optional-path-table-lba-location', cursor.uint32lsb() )
                
                cursor.seek( 156 )
                primaryAttributes.put(
                    'root-directory-record'                                                    ,
                    FileSystem__CompactDiskFileSystem__Common.read__directory_record( cursor ) ,
                    )
                
                cursor.seek( 190 )
                primaryAttributes.put( 'volume-set-identifier'   , cursor.clipped( 128 ) )
                primaryAttributes.put( 'publisher-identifier'    , cursor.clipped( 128 ) )
                primaryAttributes.put( 'data-preparer-identifier', cursor.clipped( 128 ) )
                primaryAttributes.put( 'application-identifier'  , cursor.clipped( 128 ) )
                
                primaryAttributes.put( 'copyright-file-identifier'    , cursor.clipped( 38 ) )
                primaryAttributes.put( 'abstract-file-identifier'     , cursor.clipped( 36 ) )
                primaryAttributes.put( 'bibliographic-file-identifier', cursor.clipped( 37 ) )
                
                primaryAttributes.put( 'volume-creation-date-and-time'    , cursor.clipped( 17 ) )
                primaryAttributes.put( 'volume-modification-date-and-time', cursor.clipped( 17 ) )
                primaryAttributes.put( 'volume-expiration-date-and-time'  , cursor.clipped( 17 ) )
                primaryAttributes.put( 'volume-effective-date-and-time'   , cursor.clipped( 17 ) )
                
                volumeInformation.put( 'primary-volume-descriptor', primaryAttributes )
                continue
            
            if volumeType == '\x02':
                volumeInformation.put( 'supplementary-volume-descriptor', 'present (ignored)' )
                continue
            
            if volumeType == '\x03':
                volumeInformation.put( 'volume-partition-descriptor', 'present (ignored')
                continue
            
        return volumeInformation
    
    def select( self, what ):
        
        if what == 'iso':
            volumeInformation = self._volume_information()
            
            return FileSystem__CompactDiskFileSystem__IsoDirectory(
                rado                = self._rado        ,
                volumeInformation   = volumeInformation ,
                directoryAttributes = (
                    volumeInformation
                    .get( 'primary-volume-descriptor' )
                    .get( 'root-directory-record'     )
                    )
                )
        
        return None


class FileSystem__CompactDiskFileSystem__Common():
    
    @staticmethod
    def read__directory_record( cursor ):
        directoryAttributes = Attributes()
        
        startingOffset = cursor.tell()
        
        recordLength = cursor.uint8()
        directoryAttributes.put( 'directory-record-length', recordLength )
        
        directoryAttributes.put( 'extended-attribute-record-length', cursor.uint8()     )
        directoryAttributes.put( 'lba-of-extent'                   , cursor.uint32lsb() )
        
        cursor.skip( 4 )
        
        directoryAttributes.put( 'data-length'                     , cursor.uint32lsb() )
        
        cursor.skip( 4 )
        
        directoryAttributes.put(
            'date-and-time'                                                           ,
            FileSystem__CompactDiskFileSystem__Common._read__directory_date( cursor ) ,
            )
        
        directoryAttributes.put(
            'flags'                                                                    ,
            FileSystem__CompactDiskFileSystem__Common._read__directory_flags( cursor ) ,
            )
        
        directoryAttributes.put( 'file-unit-size'        , cursor.uint8()     )
        directoryAttributes.put( 'interleave-gap-size'   , cursor.uint8()     )
        directoryAttributes.put( 'volume-sequence-number', cursor.uint16lsb() )
        
        cursor.skip( 2 )
        
        filenameLength = cursor.uint8()
        directoryAttributes.put( 'filename-length'    , filenameLength                   )
        directoryAttributes.put( 'filename-identifier', cursor.readall( filenameLength ) )
        
        # osDev says to check filenameLength, I'm checking tell instead
        # it seems to be the right thing to do, at least for the ubuntu
        # disk that I'm using as my cdfs guinea pig, anyways.
        
        if cursor.tell() & 1:
            paddingByte = cursor.uint8()
            if paddingByte != 0:
                raise Exception(
                    'wtf : non-zero padding byte?'
                    )
        
        systemUseStart  = cursor.tell() - startingOffset
        systemUseLength = recordLength - systemUseStart
        
        directoryAttributes.put( 'system-use-length', systemUseLength                   )
        directoryAttributes.put( 'system-use'       , cursor.clipped( systemUseLength ) )
        
        return directoryAttributes
    
    @staticmethod
    def _read__directory_date( cursor ):
        dateAttributes = Attributes()
        
        dateAttributes.put( 'years-since-1900', cursor.uint8() )
        dateAttributes.put( 'month'           , cursor.uint8() )
        dateAttributes.put( 'day'             , cursor.uint8() )
        dateAttributes.put( 'hour'            , cursor.uint8() )
        dateAttributes.put( 'minute'          , cursor.uint8() )
        dateAttributes.put( 'second'          , cursor.uint8() )
        dateAttributes.put( 'offset-from-gm'  , cursor.sint8() )
        
        return dateAttributes
    
    @staticmethod
    def _read__directory_flags( cursor ):
        flagsAttributes = Attributes()
        flags = cursor.uint8()
        
        flagsAttributes.put( 'hidden'                                , bool( flags & 0b00000001 ) )
        flagsAttributes.put( 'directory'                             , bool( flags & 0b00000010 ) )
        flagsAttributes.put( 'associated-file'                       , bool( flags & 0b00000100 ) )
        flagsAttributes.put( 'extended-attribute-has-format'         , bool( flags & 0b00001000 ) )
        flagsAttributes.put( 'permissions-set-in-extended-attributes', bool( flags & 0b00010000 ) )
        flagsAttributes.put( 'not-final-directory-record'            , bool( flags & 0b10000000 ) )
        
        return flagsAttributes


class FileSystem__CompactDiskFileSystem__IsoDirectory():
    name = 'file-system--compact-disk-file-system--iso-directory'
    
    def __init__( self, rado, volumeInformation, directoryAttributes ):
        self._rado                = rado
        self._volumeInformation   = volumeInformation
        self._directoryAttributes = directoryAttributes
        return
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    def list( self ):
        listing = []
        
        for directoryRecord in self._directory_records():
            filename    = directoryRecord.get( 'filename-identifier' )
            isDirectory = directoryRecord.get( 'flags' ).get( 'directory' )
            
            if filename in '\x00\x01':
                filename = { '\x00' : '.', '\x01' : '..' }[ filename ]
            
            filetype = 'directory' if isDirectory else 'file'
            
            listing.append( ( filename, filetype ) )
        
        return listing
    
    def select( self, what ):
        
        if ';' in what:
            what, _whatever = what.split( ';', 1 )
        
        for directoryRecord in self._directory_records():
            filename    = directoryRecord.get( 'filename-identifier' )
            isDirectory = directoryRecord.get( 'flags' ).get( 'directory' )
            
            if ';' in filename:
                filename, _whatever = filename.split( ';', 1 )
                
            # . matches \x00 : .. matches \x01 : FILENAME;00 and FILENAME both match same
            filename = { '\x00' : '.', '\x01' : '..' }.get( filename, filename )
            
            if filename == what:
                if isDirectory:
                    return FileSystem__CompactDiskFileSystem__IsoDirectory(
                        rado                = self._rado                ,
                        volumeInformation   = self._volumeInformation   ,
                        directoryAttributes = directoryRecord           ,
                        )
                else:
                    return FileSystem__CompactDiskFileSystem__IsoFile(
                        rado              = self._rado                ,
                        volumeInformation = self._volumeInformation   ,
                        fileAttributes    = directoryRecord           ,
                        )
            
            else:
                continue
        
        return None
    
    def _directory_records( self ):
        primaryVolumeDescriptor = self._volumeInformation.get( 'primary-volume-descriptor' )
        
        logicalBlockSize        = primaryVolumeDescriptor   .get ( 'logical-block-size' )
        extentLocation          = self._directoryAttributes .get ( 'lba-of-extent'      )
        dataLength              = self._directoryAttributes .get ( 'data-length'        )
        
        fileOffset = extentLocation * logicalBlockSize
        
        diskCursor = self._rado.cursor()
        diskCursor.seek( fileOffset )
        fileRado = diskCursor.rado( dataLength )
        fileCursor = fileRado.cursor()
        
        directoryEntryNumber = 0
        
        while fileCursor.tell() < dataLength:
            directoryEntryNumber += 1
            
            followingByte = fileCursor.uint8()
            if followingByte == 0:
                continue
            else:
                fileCursor.skip( -1 )
                yield FileSystem__CompactDiskFileSystem__Common.read__directory_record( fileCursor )


class FileSystem__CompactDiskFileSystem__IsoFile():
    name = 'file-system--compact-disk-file-system--iso-file'
    
    def __init__( self, rado, volumeInformation, fileAttributes ):
        self._rado              = rado 
        self._volumeInformation = volumeInformation
        self._fileAttributes    = fileAttributes
        return
    
    def is_listable( self ): return False
    def is_radoable( self ): return True
    
    def rado( self ):
        primaryVolumeDescriptor = self._volumeInformation .get ( 'primary-volume-descriptor' )
        logicalBlockSize        = primaryVolumeDescriptor .get ( 'logical-block-size'        )
        extentLocation          = self._fileAttributes    .get ( 'lba-of-extent'             )
        dataLength              = self._fileAttributes    .get ( 'data-length'               )
        
        fileOffset = logicalBlockSize * extentLocation
        
        diskCursor = self._rado.cursor()
        diskCursor.seek( fileOffset )
        fileRado = diskCursor.rado( dataLength )
        
        return fileRado


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- Apple Disk Image ( Disk iMaGe / .dmg file / .smi file ? ( self mounting image, <= OS-9 / .img file ? )

@model
class DiskImage__AppleDiskImage():
    name = 'disk-image--apple-disk-image'
    
    @staticmethod
    def matches( rado ):
        cursor = rado.cursor()
        cursor.end()
        cursor.skip( - 512 )
        magicKoly = cursor.read( 4 )
        return 'koly' == magicKoly
    
    def __init__( self, rado ):
        self._rado      = rado
        
        self._kolyBlock = self._koly_block()
        self._xmlData   = self._xml_data()
        
        return
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    # looks like it currently goes from dmg -> disk -> whatever we find in the disk
    # expose partitions as a list or just a rado?
    
    def list( self ):
        options = []
        
        for partition in self._find_partitions():
            options.append( ( partition['id'], partition['name'] ) )
        
        options.append( 
            ( 'xml-property-list', 'disk meta data stored in an interestingly abherent format' ) ,
            )
        
        return options
    
    def select( self, what ):
        
        for partition in self._find_partitions():
            optionName = partition['id']
            
            if what == optionName:
                return DiskImage__AppleDiskImage__Partition(
                    rado                = self._rado              ,
                    kolyBlock           = self._kolyBlock         ,
                    xmlData             = self._xmlData           ,
                    partitionDescriptor = partition['descriptor'] ,
                    )
        
        if what == 'xml-property-list':
            return ModelUnknownBlob(
                rado = self._xml_property_list_rado() ,
                )
        
        return None
    
    def _find_partitions( self ):
        partitions = []
        for _, blkxDescriptor in self._xmlData.get( 'resource-fork' ).get( 'blkx' ).items():
            
            partitionName = blkxDescriptor.get( 'CFName', None )
            if not partitionName:
                partitionName = blkxDescriptor.get( 'Name', None )
            
            if not partitionName:
                raise Exception( 'blkx dmg xml entry had no Name or CFName entry' )
            
            partitions.append({
                    'id'         : 'partition:%s' % blkxDescriptor.get( 'ID' ) ,
                    'name'       : partitionName                               ,
                    'descriptor' : blkxDescriptor                              ,
                    })
            
        # we want things that appear to be data partitions to go first so that 
        # -ff has a better chance of doing what the user wants it to automatically
        # 
        disknames = [ 'Apple_HFS' ]
        
        return sorted(
            partitions                                                                     ,
            reverse = True                                                                 ,
            key     = ( lambda v: any( diskname in v['name'] for diskname in disknames ) ) ,
            )
    
    def _koly_block( self ):
        attributes = Attributes()
        cursor     = self._rado.cursor()
        
        cursor.end()
        cursor.skip( -512 )
        
        attributes.put( 'signature'               , cursor.read( 4 )   )
        attributes.put( 'version'                 , cursor.uint32msb() )
        attributes.put( 'header-size'             , cursor.uint32msb() )
        attributes.put( 'flags'                   , cursor.uint32msb() )
        
        attributes.put( 'running-data-fork-offset', cursor.uint64msb() )
        attributes.put( 'data-fork-offset'        , cursor.uint64msb() )
        attributes.put( 'data-fork-length'        , cursor.uint64msb() )
        attributes.put( 'resource-fork-offset'    , cursor.uint64msb() )
        attributes.put( 'resource-fork-length'    , cursor.uint64msb() )
        attributes.put( 'segment-number'          , cursor.uint32msb() )
        attributes.put( 'segment-count'           , cursor.uint32msb() )
        attributes.put( 'segment-id'              , cursor.uuid()      )
        
        attributes.put( 'data-checksum-type'      , cursor.uint32msb() )
        attributes.put( 'data-checksum-size'      , cursor.uint32msb() )
        attributes.put( 'data-checksum'           , [ cursor.uint32msb()
                                                      for _ in xrange( 32 )
                                                      ])
        
        attributes.put( 'xml-offset'              , cursor.uint64msb() )
        attributes.put( 'xml-length'              , cursor.uint64msb() )
        
        # reserved
        cursor.skip( 120 )
        
        attributes.put( 'checksum-type'           , cursor.uint32msb() )
        attributes.put( 'checksum-size'           , cursor.uint32msb() )
        attributes.put( 'checksum'                , [ cursor.uint32msb()
                                                      for _ in xrange( 32 )
                                                      ])
        
        attributes.put( 'image-variant'           , cursor.uint32msb() )
        attributes.put( 'sector-count'            , cursor.uint64msb() )
        
        attributes.put( 'reserved-1'              , cursor.uint32msb() )
        attributes.put( 'reserved-2'              , cursor.uint32msb() )
        attributes.put( 'reserved-3'              , cursor.uint32msb() )
        
        return attributes
    
    def _xml_property_list_rado( self ):
        return RadoRado(
            name   = 'xml-property-list-rado'            ,
            rado   = self._rado                          ,
            offset = self._kolyBlock.get( 'xml-offset' ) ,
            size   = self._kolyBlock.get( 'xml-length' ) ,
            )
    
    def _xml_data( self ):
        # apple stuffed binary data in base64 in xml in binary data
        # the xml is not well formed, but positionally dependent cruft
        # the data in the base64 is the binary structures detailing the
        #   disk layout
        # 
        # take this moment to give a slow clap for apple
        # 
        # take another moment to give me one for breaking out regexes :)
        
        # since we're slurping the entire thing, a size check might be in order here
        # but I don't care right now so that will be left as an excercise for the reader
        
        propertyListRado = self._xml_property_list_rado()
        propertyList     = propertyListRado.cursor().read( propertyListRado.size() )
        
        rxXmlExploder      = re.compile( '<("[^"]*"|[^>])*>|[^<]*' )
        
        rxTagExploder      = re.compile(
            '(?P<name>/?\\w+)'              # name; wordiness with optionally prefixed slash
            '\\s*'                          # optional whitespace
            '(?:'                           # start optional assignment
              '='                             # assignment
              '\\s*'                          # optional whitespace
              '(?P<quote>"|\')?'              # optional quote
              '(?P<value>'                      # value
                '(?(quote)'                     # if quote
                   '(?:(?!(?P=quote)).)*'         # any number of not quote
                   '|'                          # else
                   '(?:(?!>)\\s)*'                # any number of ( not endtag and not spacey )
                ')'                             # end if quote
              ')'                             # end value
              '(?P=quote)'                    # quote again if quote
            ')?'                            # end optional assignment
            )
        
        # a stack for building up the document as nested attributes
        document = []
        
        for m in rxXmlExploder.finditer( propertyList ):
            maybeTag = m.group()
            
            if maybeTag and maybeTag[0] == '<':
                tagBits = [ mm.group( 'name', 'value' ) for mm in rxTagExploder.finditer( maybeTag ) ]
            else:
                tagBits = None
                
            if not document:
                # allow dict
                
                if not tagBits:
                    continue
                
                if tagBits[0][0] in [ 'xml', 'DOCTYPE', 'plist']:
                    continue
                
                if tagBits[0][0] == 'dict':
                    document.append( ('dict', Attributes(), [] ) )
                    continue
                
                raise Exception( 'unknown tagbits out of document %s' % repr( tagBits ) )
                
            if not tagBits:
                if not document:
                    continue
                elif not maybeTag.strip():
                    continue
                elif document[-1][0] in ['key', 'string', 'data']:
                    document[-1][1].append( maybeTag )
                    continue
                else:
                    raise Exception(
                        'unknown document given data : %s' % repr( document[-1] )
                        )
            
            if tagBits[0][0] == 'dict':
                document.append( ('dict', Attributes(), [] ) )
                continue
            
            if tagBits[0][0] == 'array':
                document.append( ('array', Attributes() ) )
                continue
            
            if tagBits[0][0] in ['string', 'key', 'data' ]:
                document.append( ( tagBits[0][0], []) )
                continue
            
            if tagBits[0][0][0] == '/':
                if not document[-1][0] == tagBits[0][0][1:]: raise Exception( 'non-matching tags in xml' )
                
                if len( document ) == 1:
                    return document[-1][1]
                
                if document[-1][0] == 'key' and document[-2][0] == 'dict':
                    if document[-2][2]: raise Exception( 'key already pending for dict' )
                    document[-2][2].append( ''.join( document[-1][1] ) )
                    document.pop()
                    continue
                
                if document[-1][0] in ['string','data','array','dict'] and document[-2][0] == 'dict':
                    if not document[-2][2]: raise Exception( 'no key pending for dict' )
                    if document[-1][0] in ['string','data']:
                        document[-2][1].put( document[-2][2][0], ''.join( document[-1][1] ) )
                    elif document[-1][0] in ['array','dict']:
                        document[-2][1].put( document[-2][2][0], document[-1][1] )
                    else:
                        raise Exception( 'impossible error' )
                    document[-2][2].pop()
                    document.pop()
                    continue
                
                if document[-1][0] == 'dict' and document[-2][0] == 'array':
                    document[-2][1].append( document[-1][1] )
                    document.pop()
                    continue
                
                raise Exception( ' no handler for closing document %s in document %s' % (
                        repr( document[-1] )[:50],
                        repr( document[-2] )[:50],
                        ))
            
            raise Exception( 'no handler for tagbits %s' % repr( tagBits )[:50] )
        
        # actual return occurs in mess above
        raise Exception( 'wot m8' )


class DiskImage__AppleDiskImage__Partition():
    def __init__( self, rado, kolyBlock, xmlData, partitionDescriptor ):
        self._rado                = rado
        self._kolyBlock           = kolyBlock
        self._xmlData             = xmlData
        self._partitionDescriptor = partitionDescriptor
        return
    
    def is_listable( self ): return False
    def is_radoable( self ): return True
    
    def rado( self ):
        return RadoBlock(
            name        = 'dmg-partition-block-device-rado' ,
            blockDevice = DiskImage__AppleDiskImage__Partition__BlockDevice(
                rado                = self._rado                   ,
                kolyBlock           = self._kolyBlock              ,
                xmlData             = self._xmlData                ,
                partitionDescriptor = self._partitionDescriptor    ,
                ))

class DiskImage__AppleDiskImage__Partition__BlockDevice():
    def __init__( self, rado, kolyBlock, xmlData, partitionDescriptor ):
        self._rado                = rado
        self._kolyBlock           = kolyBlock
        self._xmlData             = xmlData
        self._partitionDescriptor = partitionDescriptor
        
        self._runMap    = self._build_run_map( self._partitionDescriptor )
        
        return
    
    def block_size( self ):
        # I think dmg always has 512 byte blocks
        # 
        return 512
    
    def size( self ):
        return self._kolyBlock.get( 'data-fork-length' )
    
    @Common.memoize( 'apple-disk-image-blocks' )
    def get_block( self, blockNo ):
        
        for run in self._runMap:
            if run.contains( blockNo ):
                sectorRado = run.get_sector_rado( blockNo )
                # print 'run:%s rado:%s' % (
                #     repr( run        ) ,
                #     repr( sectorRado ) ,
                #     )
                return sectorRado
        
        raise Exception( 'wat block : %s' % repr( blockNo ) )
    
    def _build_run_map( self, partitionDescriptor ):
        
        driveRuns = []
        
        blockAttributes = (
            self._block_attributes( RadoBlob(
                    name = 'block-attribute-decode-blob-rado'                  ,
                    blob = base64.b64decode( partitionDescriptor.get('Data') ) ,
                    )))
        
        for _, blockRun in blockAttributes.get( 'block-run-entries' ).items():
            if blockRun.get( 'sector-count' ) == 0:
                continue
            
            sectorKlass = {
                0x00000000 : DiskImage__AppleDiskImage__ZeroFill__Run     ,
                0x00000001 : DiskImage__AppleDiskImage__Uncompressed__Run ,
                0x00000002 : DiskImage__AppleDiskImage__ZeroFill__Run     , # unknown in resources?
                0x80000004 : DiskImage__AppleDiskImage__UDCO__Run         , # apple disk compression
                0x80000005 : DiskImage__AppleDiskImage__UDZO__Run         , # zlib compression
                0x80000006 : DiskImage__AppleDiskImage__UDBZ__Run         , # bz2lib compression
                }.get( blockRun.get( 'entry-type' ), None )
            
            if not sectorKlass:
                raise Exception(
                    'unknown dmg sector run type : %s ( %s )' % (
                        repr( blockRun.get( 'entry-type' ) )        ,
                        repr( hex( blockRun.get( 'entry-type' ) ) ) ,
                        ))
            
            # print '# sector klass : %s' % repr( sectorKlass )
            driveRuns.append(
                sectorKlass(
                    sectorNo    = blockRun.get( 'sector-number' ) ,
                    sectorCount = blockRun.get( 'sector-count'  ) ,
                    rado        = RadoRado(
                        name   = 'dmg-compressed-run-rado'           ,
                        rado   = self._rado                          ,
                        offset = blockRun.get( 'compressed-offset' ) ,
                        size   = blockRun.get( 'compressed-length' ) ,
                        )))
            
        return driveRuns
    
    def _block_attributes( self, rado ):
        attributes = Attributes()
        cursor = rado.cursor()
        
        attributes.put( 'signature'             , cursor.read( 4 )   )
        attributes.put( 'version'               , cursor.uint32msb() )
        attributes.put( 'sector-number'         , cursor.uint64msb() )
        attributes.put( 'sector-count'          , cursor.uint64msb() )
        attributes.put( 'data-offset'           , cursor.uint64msb() )
        attributes.put( 'buffers-needed'        , cursor.uint32msb() )
        attributes.put( 'block-descriptors'     , cursor.uint32msb() )
        
        attributes.put( 'reserved'              , [ cursor.uint32msb()
                                                    for _ in xrange( 6 )
                                                    ])
        
        attributes.put( 'checksum'              , self._read__udif_checksum( cursor ) )
        
        numberOfBlockChunks = cursor.uint32msb()
        attributes.put( 'number-of-block-chunks', numberOfBlockChunks )
        
        blkxRunEntries = Attributes()
        
        while True:
            runEntry = self._read__blkx_run_entry( cursor )
            blkxRunEntries.append( runEntry )
            if runEntry.get( 'entry-type' ) == 0xffffffff:
                # terminal blkxRun entry
                break
        
        attributes.put( 'block-run-entries'     , blkxRunEntries )
        
        return attributes
    
    def _read__udif_checksum( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'type' , cursor.uint32msb() )
        attributes.put( 'size' , cursor.uint32msb() )
        attributes.put( 'data' , [ cursor.uint32msb()
                                   for _ in xrange( 32 )
                                   ])
        return attributes
    
    def _read__blkx_run_entry( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'entry-type'        , cursor.uint32msb() )
        attributes.put( 'comment'           , cursor.read( 4 )   )
        attributes.put( 'sector-number'     , cursor.uint64msb() )
        attributes.put( 'sector-count'      , cursor.uint64msb() )
        attributes.put( 'compressed-offset' , cursor.uint64msb() )
        attributes.put( 'compressed-length' , cursor.uint64msb() )
        
        return attributes
    

class DiskImage__AppleDiskImage__ZeroFill__Run():
    def __init__( self, rado, sectorNo, sectorCount ):
        # provides a rado filled with null bytes
        # 
        self._sectorNo    = sectorNo
        self._sectorCount = sectorCount
        
        # again, depending on universal dmg 512 block size
        # 
        self._data        = '\0' * 512 # immutable zero'd sector of data, shared between all sectors
        return
    
    def contains( self, sectorNo ):
        if sectorNo >= self._sectorNo and sectorNo < self._sectorNo + self._sectorCount:
            return True
        else:
            return False
    
    def get_sector_rado( self, sectorNo ):
        if not self.contains( sectorNo ): raise Exception( 'asked run for uncontained sector' )
        
        if sectorNo >= self._sectorNo and sectorNo < self._sectorNo + self._sectorCount:
            return RadoBlob(
                name = 'zerofill-fake-rado' ,
                blob = self._data           ,
                )


class DiskImage__AppleDiskImage__Uncompressed__Run():
    
    def __init__( self, rado, sectorNo, sectorCount ):
        self._rado        = rado
        self._sectorNo    = sectorNo
        self._sectorCount = sectorCount
        return
    
    def contains( self, sectorNo ):
        return (
            ( sectorNo >= self._sectorNo )
            and
            ( sectorNo < self._sectorNo + self._sectorCount )
            )
    
    def get_run_no( self ):
        return self._runNo
    
    def get_sector( self, sectorNo ):
        # should just be able to ( sectorNo - self._sectorNo ) * 512 into the rado and done
        # 
        if not self.contains( sectorNo ):
            raise Exception( 'asked run for uncontained sector' )
        
        c = self._rado.cursor()
        c.seek( ( sectorNo - self._sectorNo ) * 512 )
        return c.read( 512 )

class DiskImage__AppleDiskImage__UDCO__Run():
    # Apple Data Compression
    def __init__( self, rado ):
        raise Exception( 'unimplemented' )


class DiskImage__AppleDiskImage__UDZO__Run():
    # zlib Compression
    def __init__( self, rado, sectorNo, sectorCount ):
        self._rado        = rado
        self._sectorNo    = sectorNo
        self._sectorCount = sectorCount
        return
    
    def contains( self, sectorNo ):
        if sectorNo >= self._sectorNo and sectorNo < self._sectorNo + self._sectorCount:
            return True
        else:
            return False
    
    def get_sector_rado( self, sectorNo ):
        if not self.contains( sectorNo ): raise Exception( 'asked run for uncontained sector' )
        
        closestSector = DiskImage__AppleDiskImage__UDZO__Sector(
            run              = self                 ,
            sectorNo         = self._sectorNo       ,
            decompressobj    = zlib.decompressobj() ,
            compressedOffset = 0                    ,
            )
        
        while closestSector.get_sector_no() < sectorNo:
            closestSector = closestSector.next_sector()
        
        if not closestSector.get_sector_no() == sectorNo:
            raise Exception( 'wat' )
        
        return closestSector.rado()
        
        # just a reminder to recreate the closest-to cache so we can restart runs from 
        # closer than the beginning of the run
        
        # this section could eventually be sped up by the introduction of a binary lookup tree
        # possibly inside of the sector cache
        
        # closest sector could be the desired sector, though since we check the cache
        # before ever getting here, it never will be, but we'll program as though it could
        
        # closestSector = None
        # for cachedSector in self._sectorCache.get_sectors_in_run( self._runNo ):
        #     if cachedSector.get_sector_no() < sectorNo:
        #          if (
        #              ( not closestSector )
        #              or
        #              ( cachedSector.get_sector_no() > closestSector.get_sector_no() )
        #              ):
        #             closestSector = cachedSector
        # 
        # if not closestSector:
        #     closestSector = DiskImage__AppleDiskImage__UDZO__Sector(
        #         run              = self                 ,
        #         sectorNo         = self._sectorNo       ,
        #         decompressobj    = zlib.decompressobj() ,
        #         compressedOffset = 0                    ,
        #         )
        # 
        # while closestSector.get_sector_no() < sectorNo:
        #     closestSector = closestSector.next_sector()
        # 
        # # we have the sector we desired
        # 
        # self._sectorCache.cache( closestSector )
        # return closestSector
        

class DiskImage__AppleDiskImage__UDZO__Sector():
    def __init__( self, run, sectorNo, decompressobj, compressedOffset ):
        # decompress 512 bytes of data from compressedPosition
        # we represent sectorNo
        self._run              = run
        self._sectorNo         = sectorNo
        self._decompressobj    = decompressobj
        self._compressedOffset = compressedOffset
        
        self._data             = None
        self._decompress_data() # fills data, advances compressedOffset
    
    def _decompress_data( self ):
        # reading the amount compressed we want uncompressed guarantees
        # the decompressor will have enough data to always return a full
        # block
        
        # note that the "unconsumed_tail" isn't the same as the uncompressed data
        # apparently its altered ( and expanded ) by the decompression, so we can't
        # just count the size and pass it on, we have to account for it properly
        
        oldData         = self._decompressobj.unconsumed_tail
        newDataRequired = 512 - len( oldData )
        
        cursor = self._run._rado.cursor()
        cursor.skip( self._compressedOffset )
        compressedData = cursor.read( newDataRequired )
        
        self._data = self._decompressobj.decompress( oldData + compressedData, 512 )
        
        # print 'DECOMPRESSED', repr( self._data )
        
        self._compressedOffset += newDataRequired
        
        return
    
    def rado( self ):
        return RadoBlob(
            name = 'dmg-udzo-run-sector-rado' ,
            blob = self._data                 ,
            )
    
    def get_sector_no( self ):
        return self._sectorNo
    
    def next_sector( self ):
        if not self._run.contains( self._sectorNo + 1 ):
            raise Exception( 'cannot advance to next sector, not in same run' )
        
        return DiskImage__AppleDiskImage__UDZO__Sector(
            run              = self._run                  ,
            sectorNo         = self._sectorNo + 1         ,
            decompressobj    = self._decompressobj.copy() ,
            compressedOffset = self._compressedOffset     ,
            )
    
    def get_data( self ):
        return self._data


class DiskImage__AppleDiskImage__UDBZ__Run():
    def __init__( self, rado, sectorNo, sectorCount, sectorCache ):
        raise Exception( 'unimplemented' )


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- Apple HFS ( High-perforance File System )

@model
class FileSystem__AppleHfsPlus():
    name = 'file-system--apple-hfs-plus'
    
    @staticmethod
    def matches( rado ):
        cursor = rado.cursor()
        cursor.skip( 1024 )
        signature = cursor.read( 2 )
        return signature == 'H+'
    
    def __init__( self, rado ):
        self._rado = rado
        
        self._volumeHeader = self._volume_header()
        
        print self._volumeHeader
        
        self._catalogForkRado  = RadoBlock(
            name        = 'hfs-catalog-fork' ,
            blockDevice = FileSystem__AppleHfsPlus__ForkDataStructure__BlockDevice(
                extents = self._volumeHeader.get( 'catalog-file' ).get( 'extents' ) ,
                rado    = self._rado                                                ,
                ))
        
        # print 'CATALOG-FORK-RADO-DATA'
        # catalogCursor = self._catalogForkRado.cursor()
        # catalogCursor.end()
        # catalogSize = catalogCursor.tell()
        # catalogCursor.seek( 0 )
        # print repr( catalogCursor.read( catalogSize ) )
        
        self._catalogBTree = FileSystem__AppleHfsPlus__BTree(
            rado = self._catalogForkRado ,
            )
        
        raise Exception( 'wat' )
        
        return
    
    def is_listable( self ): return False
    def is_radoable( self ): return False
    
    def _volume_header( self ):
        attributes = Attributes()
        cursor = self._rado.cursor()
        
        cursor.seek( 1024 )
        attributes.put( 'signature'           , cursor.readall( 2 ) )
        attributes.put( 'version'             , cursor.uint16msb()  )
        attributes.put( 'attributes'          , cursor.uint32msb()  )
        attributes.put( 'last-mounted-version', cursor.uint32msb()  )
        attributes.put( 'journal-info-block'  , cursor.uint32msb()  )
        
        attributes.put( 'create-date'         , cursor.uint32msb()  )
        attributes.put( 'modify-date'         , cursor.uint32msb()  )
        attributes.put( 'backup-date'         , cursor.uint32msb()  )
        attributes.put( 'checked-date'        , cursor.uint32msb()  )
        
        attributes.put( 'file-count'          , cursor.uint32msb()  )
        attributes.put( 'folder-count'        , cursor.uint32msb()  )
        
        attributes.put( 'block-size'          , cursor.uint32msb()  )
        attributes.put( 'total-blocks'        , cursor.uint32msb()  )
        attributes.put( 'free-blocks'         , cursor.uint32msb()  )
        
        attributes.put( 'next-allocation'     , cursor.uint32msb()  )
        attributes.put( 'rsrc-clump-size'     , cursor.uint32msb()  )
        attributes.put( 'data-clump-size'     , cursor.uint32msb()  )
        attributes.put( 'next-catalog-id'     , cursor.uint32msb()  )
        
        attributes.put( 'write-count'         , cursor.uint32msb()  )
        attributes.put( 'encodings-bitmap'    , cursor.uint64msb()  )
        
        attributes.put( 'finder-info'         , Attributes.from_iterator(
                cursor.uint32msb() for _ in xrange( 8 )
                ))
        
        attributes.put( 'allocation-file'     , self._read__hfs_plus_fork_data( cursor ) )
        attributes.put( 'extents-file'        , self._read__hfs_plus_fork_data( cursor ) )
        attributes.put( 'catalog-file'        , self._read__hfs_plus_fork_data( cursor ) )
        attributes.put( 'attributes-file'     , self._read__hfs_plus_fork_data( cursor ) )
        attributes.put( 'startup-file'        , self._read__hfs_plus_fork_data( cursor ) )
        
        return attributes
    
    def _read__hfs_plus_fork_data( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'logical-size', cursor.uint64msb() )
        attributes.put( 'clump-size'  , cursor.uint32msb() )
        attributes.put( 'total-blocks', cursor.uint32msb() )
        attributes.put( 'extents'     , Attributes.from_iterator(
                self._read__hfs_plus_extent_descriptor( cursor )
                for _ in xrange( 8 )
                ))
        
        return attributes
    
    def _read__hfs_plus_extent_descriptor( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'start-block', cursor.uint32msb() )
        attributes.put( 'block-count', cursor.uint32msb() )
        
        return attributes
    
class FileSystem__AppleHfsPlus__ForkDataStructure__BlockDevice():
    def __init__( self, extents, rado ):
        self._extents = extents
        self._rado    = rado
        return
    
    def block_size( self ):
        # assumed due to madness
        # 
        return 512 # ????
    
    def size( self ):
        blocks = 0
        for _, extent in self._extents.items():
            blockCount = extent.get( 'block-count' )
            if blockCount:
                blocks += blockCount
            else:
                break
        else:
            # I think this is the first time I've ever used the python
            # for else structure. behold, the glory
            # ( the else executes if the for doesn't break, it's a counter intuitive thing )
            # 
            raise Exception(
                'blocks exceed first eight extents, extended extents unimplemented'
                )
        
        return blocks * self.block_size()
    
    # memoize?
    # 
    def get_block( self, blockNo ):
        blockOffset = self._get_block_offset( blockNo )
        cursor      = self._rado.cursor()
        cursor.skip( blockOffset )
        return cursor.rado( size = 512 )
    
    def _get_block_offset( self, blockNo ):
        # 0-indexed, baby
        
        # we're going to go out on a limb here, and assume that block-size, aka allocation block size,
        # is _not_ related to the logical block size in HFS+, which we will assume remains
        # the same 512 per block as the usually underlying dmg format
        # 
        # because this shit just isn't working and we need to try something
        # 
        
        extentThus = 0
        for _, extent in self._extents.items():
            
            diskStartBlock = extent.get( 'start-block' )
            blockCount     = extent.get( 'block-count' )
            blockSize      = self.block_size()           # self._volumeHeader.get( 'block-size' )
            
            print 'extent-start:%s <= block-number:%s <= extent-start+block-count:%s' % (
                repr( extentThus              ) ,
                repr( blockNo                 ) ,
                repr( extentThus + blockCount ) ,
                )
                
            if extentThus <= blockNo <= extentThus + blockCount:
                selectedBlock = ( diskStartBlock + ( blockNo - extentThus ) )
                diskOffset    = selectedBlock * blockSize
                
                print 'extentThus:%s blockNo:%s selectedBlock:%s diskOffset:%s' % (
                    repr( extentThus    ) ,
                    repr( blockNo       ) ,
                    repr( selectedBlock ) ,
                    repr( diskOffset    ) ,
                    )
                
                return diskOffset
            else:
                extentThus += blockCount
        
        raise Exception( 'block not in first eight extents, extended extents unimplemented' )


class FileSystem__AppleHfsPlus__BTree():
    
    def __init__( self, rado ):
        self._rado = rado
        
        self._nodeDescriptor = self._node_descriptor()
        
        print 'NODE-DESCRIPTOR'
        print self._nodeDescriptor
        
        raise Exception( 'extent' )
        return
    
    def _node_descriptor( self ):
        cursor = self._rado.cursor()
        attributes = Attributes()
        
        attributes.put( 'f-link'     , cursor.uint32msb() )
        attributes.put( 'b-link'     , cursor.uint32msb() )
        attributes.put( 'kind'       , cursor.sint8()     )
        attributes.put( 'height'     , cursor.uint8()     )
        attributes.put( 'num-records', cursor.uint16msb() )
        attributes.put( 'reserved'   , cursor.uint16msb() )
        
        return attributes


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- qcow2 - a compressed block storage format, can also store changed blocks from different source
#-
#- https://people.gnome.org/~markmc/qcow-image-format.html

@model
class StorageFormat__QCOW2():
    name = 'storage-format--qcow2'
    
    @staticmethod
    def matches( rado ):
        cursor = rado.cursor()
        magic = cursor.read( 4 )
        return ( magic == 'QFI\xfb' )
    
    def __init__( self, rado ):
        self._rado = rado
        return
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    def list( self ):
        # in future, this format will also need support for snapshot traversal
        return [ ( 'main-image', 'the primary image in the file, as opposed to any snapshots') ]
    
    def select( self, what ):
        # snapshots not yet implemented
        
        if what == 'main-image':
            return StorageFormat__QCOW2__MainImage( self._rado )
        
        return None


class StorageFormat__QCOW2__MainImage():
    name = 'storage-format--qcow2--main-image--rado'
    
    def __init__( self, rado ):
        self._rado = rado
        return
    
    def is_listable( self ): return False
    def is_radoable( self ): return True
    
    def rado( self ):
        return RadoBlock( 
            StorageFormat__QCOW2__MainImage__BlockDevice( self._rado )
            )


class StorageFormat__QCOW2__MainImage__BlockDevice():
    
    _FLAG_COPIED     = 1 << 63
    _FLAG_COMPRESSED = 1 << 62
    
    def __init__( self, rado ):
        self._rado        = rado
        self._header      = self._read_header()
        self._backingRado = None
        
        if not ( self._header.get('version') == 2 ):
            raise Exception(
                'currently only handles qcow2 version 2'
                )
        
        if not ( self._header.get('crypt-method') == 0 ):
            raise Exception(
                'encryption not currently accounted for'
                )
        
        if not ( self._header.get('backing-file-offset') == 0 ):
            cursor = self._rado.cursor()
            cursor.seek( self._header.get( 'backing-file-offset' ) )
            backingFilePath = cursor.read( self._header.get( 'backing-file-size'   ) )
            
            print '# OPENING QCOW2 BACKING FILE', repr( backingFilePath )
            backingRado = RadoBlock(
                File__BlockDevice(
                    open( backingFilePath )
                    ))
            
            print '# SCANNING BACKING RADO FOR SCANABLE TYPE'
            print '# NEED A WAY TO SPECIFY THIS SO WE CAN CHOOSE RAW OR WHATEVER'
            potentialModels = list(
                modelMatch[ 1 ]
                for modelMatch in determine_compatible_models( backingRado )
                if modelMatch[ 0 ]
                )
            
            print '# POTENTIAL MODELS', repr( potentialModels )
            
            if len( potentialModels ) > 1:
                raise Exception( 
                    'cannot decide backing file model; multiple : %s' % repr( potentialModels )
                    )
            
            potentialModel = potentialModels[ 0 ]( backingRado )
            
            if not potentialModel.is_radoable():
                # we use the first ( thus default ) selection if listable, only once to find a radoable
                print '# MATCHING MODEL NOT SCANNABLE, CHECKING FIRST OPTION'
                firstOption    = potentialModel.list()[0][0]
                print '# FIRST OPTION', repr( firstOption )
                firstOptionModel = potentialModel.select( firstOption )
                if firstOptionModel.is_radoable():
                    print '# FIRST OPTION IS SCANNABLE, USING THAT'
                    potentialModel = firstOptionModel
            
            if potentialModel.is_radoable():
                print '# USING RADO FROM', repr( potentialModels[0] )
                self._backingRado = potentialModel.rado()
                
            else:
                print '# USING BACKING FILE AS RAW'
                self._backingRado = backingRado
                
        return
    
    def size( self ):
        return self._header.get( 'size' )
    
    def block_size( self ):
        return 512
    
    @Common.memoize( 'qcow2-blocks' )
    def get_block( self, blockNo ):
        
        clusterBits         = self._header.get( 'cluster-bits' )
        levelTwoBits        = clusterBits - 3
        levelOneBits        = 64 - 2 - levelTwoBits - clusterBits
        
        levelOneMask = ( ( 1 << ( levelOneBits ) ) - 1 ) << ( levelTwoBits + clusterBits )
        levelTwoMask = ( ( 1 << ( levelTwoBits ) ) - 1 ) << clusterBits
        clusterMask  = ( ( 1 << ( clusterBits  ) ) - 1 )
        
        blockAddress  = blockNo * self.block_size()
        
        levelOneIndex = ( blockAddress & levelOneMask ) >> ( levelTwoBits + clusterBits )
        levelTwoIndex = ( blockAddress & levelTwoMask ) >> clusterBits
        clusterIndex  = ( blockAddress & clusterMask  )
        
        cursor = self._rado.cursor()
        
        levelOneTableOffset = self._header.get( 'l1-table-offset' )
        
        cursor.seek( levelOneTableOffset )
        cursor.skip( levelOneIndex * 8   )
        levelTwoTableOffsetAndFlags = cursor.uint64msb()
        
        levelTwoCopied     = levelTwoTableOffsetAndFlags & self._FLAG_COPIED
        levelTwoCompressed = levelTwoTableOffsetAndFlags & self._FLAG_COMPRESSED
        
        # ignore copied, it's just an optimization thing
        
        if levelTwoCompressed:
            raise Exception( 'qcow2 compressed level two tables unimplemented' )
        
        levelTwoTableOffset = levelTwoTableOffsetAndFlags & ~( self._FLAG_COPIED | self._FLAG_COMPRESSED )
        
        if levelTwoTableOffset == 0:
            # the desired block is not currently allocated
            if not self._backingRado:
                DEBUG_BACKING( '# ~~ UNALLOCATED BLOCK REQUESTED, NO BACKING RADO, USING ZEROED' )
                return RadoZero( self.block_size() )
            else:
                DEBUG_BACKING( '# ~~ UNALLOCATED BLOCK REQUESTED, DEFERRING TO BACKING RADO' )
                backingCursor = self._backingRado.cursor()
                backingCursor.seek( blockNo * self.block_size() )
                return backingCursor.rado( self.block_size() )
            
        cursor.seek( levelTwoTableOffset )
        cursor.skip( levelTwoIndex * 8   )
        clusterOffsetAndFlags = cursor.uint64msb()
        
        clusterCopied     = clusterOffsetAndFlags & self._FLAG_COPIED
        clusterCompressed = clusterOffsetAndFlags & self._FLAG_COMPRESSED
        clusterOffset     = clusterOffsetAndFlags & ~( self._FLAG_COPIED | self._FLAG_COMPRESSED )
        
        if clusterCompressed:
            raise Exception( 'qcow2 compressed cluster unimplemented' )
        
        if clusterOffset == 0:
            if not self._backingRado:
                DEBUG_BACKING( '# ~~ UNALLOCATED CLUSTER, NO BACKING RADO, USING ZEROED' )
                return RadoZero( self.block_size() )
            else:
                DEBUG_BACKING( '# ~~ UNALLOCATED CLUSTER, DEFERRING TO BACKING RADO' )
                backingCursor = self._backingRado.cursor()
                backingCursor.seek( blockNo * self.block_size() )
                return backingCursor.rado( self.block_size() )
        
        blockAddress = clusterOffset + clusterIndex
        
        cursor.seek( blockAddress )
        blockRado = cursor.rado( self.block_size() )
        
        return blockRado
        
    def _read_header( self ):
        cursor     = self._rado.cursor()
        
        attributes = Attributes()
        
        attributes.put( 'magic'                  , cursor.read( 4 )   )
        attributes.put( 'version'                , cursor.uint32msb() )
        
        attributes.put( 'backing-file-offset'    , cursor.uint64msb() )
        attributes.put( 'backing-file-size'      , cursor.uint32msb() )
        
        attributes.put( 'cluster-bits'           , cursor.uint32msb() )
        attributes.put( 'size'                   , cursor.uint64msb() )
        attributes.put( 'crypt-method'           , cursor.uint32msb() )
        
        attributes.put( 'l1-size'                , cursor.uint32msb() ) # number 8 byte entries in table
        attributes.put( 'l1-table-offset'        , cursor.uint64msb() ) # offset to start of table
        
        attributes.put( 'refcount-table-offset'  , cursor.uint64msb() )
        attributes.put( 'refcount-table-clusters', cursor.uint32msb() )
        
        attributes.put( 'nb-snapshots'           , cursor.uint32msb() )
        attributes.put( 'snapshots-offset'       , cursor.uint64msb() )
        
        return attributes
    
        


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- classic DOS-style master boot record disk format
#- 


@model
class DiskFormat__MasterBootRecord():
    name = 'disk-format--master-boot-record'
    
    @staticmethod
    def matches( rado ):
        cursor = rado.cursor()
        cursor.seek( 510 )
        magic = cursor.read( 2 )
        return magic == '\x55\xaa'
    
    def __init__( self, rado ):
        self._rado = rado
        self._mbr  = self._read_mbr()
        return
    
    def _read_mbr( self ):
        cursor     = self._rado.cursor()
        attributes = Attributes()
        
        cursor.seek( 0x01b4 )
        attributes.put( 'unique-disk-id', cursor.read( 10 ) )
        
        attributes.put(
            'partition-entries',
            Attributes.from_iterator(
                self._read_partition_entry_from( cursor )
                for _ in xrange( 4 )
                ))
        
        attributes.put( 'magic', cursor.read( 2 ) )
        
        return attributes
    
    def _read_partition_entry_from( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'bootable-flag'                , cursor.uint8()     )
        attributes.put( 'starting-head-sector-cylinder', cursor.read( 3 )   )
        attributes.put( 'system-id'                    , cursor.uint8()     )
        attributes.put( 'ending-head-sector-cylinder'  , cursor.read( 3 )   )
        attributes.put( 'relative-sector'              , cursor.uint32lsb() )
        attributes.put( 'total-sectors'                , cursor.uint32lsb() )
        
        return attributes
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    def list( self ):
        rv = []
        for partitionNo, partitionAttributes in self._mbr.get( 'partition-entries' ).items():
            if partitionAttributes.get( 'total-sectors' ) != 0:
                rv.append((
                        'partition-%s' % str( partitionNo ) ,
                        ':system-id=%s,bootable=%s,sectors=%s' % (
                            hex( partitionAttributes.get( 'system-id'     ) ) ,
                            hex( partitionAttributes.get( 'bootable-flag' ) ) ,
                            str( partitionAttributes.get( 'total-sectors' ) ) ,
                            )))
        
        return rv
    
    def select( self, what ):
        for partitionNo, partitionAttributes in self._mbr.get( 'partition-entries' ).items():
            if partitionAttributes.get( 'total-sectors' ) != 0:
                if ('partition-%s' % partitionNo ) == what:
                    return DiskFormat__MasterBootRecord__Partition(
                        rado                = self._rado          ,
                        partitionNo         = partitionNo         ,
                        partitionAttributes = partitionAttributes ,
                        )

class DiskFormat__MasterBootRecord__Partition():
    def __init__( self, rado, partitionNo, partitionAttributes ):
        self._rado                = rado
        self._partitionNo         = partitionNo
        self._partitionAttributes = partitionAttributes
        return
    
    def is_listable( self ): return False
    def is_radoable( self ): return True
    
    def rado( self ):
        cursor = self._rado.cursor()
        cursor.seek( 512 * self._partitionAttributes.get( 'relative-sector' ) )
        return cursor.rado( size = self._partitionAttributes.get( 'total-sectors' ) * 512 )


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- ext2/3/4 file system -- they're backwards compatible
#- 
#- https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout

@model
class FileSystem__Ext():
    name = 'file-system--ext'
    
    @staticmethod
    def matches( rado ):
        cursor = rado.cursor()
        cursor.seek( 1024 + 56 )
        magic = cursor.read( 2 )
        return magic == '\x53\xef'
    
    def __init__( self, rado ):
        self._rado       = rado
        self._superblock = self._read_superblock()
        return
    
    def is_radoable( self ): return False
    def is_listable( self ): return True
    
    def list( self ):
        return [ ('root', 'the root of the filesystem' ) ]
    
    def select( self, what ):
        if what == 'root':
            return FileSystem__Ext__Directory(
                fileSystemExt = self                       ,
                inodeNo       = self._ROOT_DIRECTORY_INODE ,
                )
        
        return None
    
    _ROOT_DIRECTORY_INODE = 2   # which inode holds the root directories inode
    _GOOD_OLD_INODE_SIZE  = 0   # flag to use previous standard inode size ( 1 is dynamic inode size )
    
    def _read_superblock( self ):
        cursor     = self._rado.cursor()
        attributes = Attributes()
        
        cursor.seek( 1024 )
        
        attributes.put( 'inodes-count'         , cursor.uint32lsb() )
        attributes.put( 'blocks-count-lo'      , cursor.uint32lsb() )
        
        # number blocks allocatable by superuser only
        attributes.put( 'r-blocks-count-lo'    , cursor.uint32lsb() ) 
        attributes.put( 'free-blocks-count-lo' , cursor.uint32lsb() )
        attributes.put( 'free-inodes-count'    , cursor.uint32lsb() )
        attributes.put( 'first-data-block'     , cursor.uint32lsb() )
        attributes.put( 'log-block-size'       , cursor.uint32lsb() )
        attributes.put( 'log-cluster-size'     , cursor.uint32lsb() )
        attributes.put( 'blocks-per-group'     , cursor.uint32lsb() )
        attributes.put( 'clusters-per-group'   , cursor.uint32lsb() )
        attributes.put( 'inodes-per-group'     , cursor.uint32lsb() )
        
        attributes.put( 'mtime'                , cursor.uint32lsb() ) # mount time since epoch
        attributes.put( 'wtime'                , cursor.uint32lsb() ) # write time since epoch
        
        attributes.put( 'mnt-count'            , cursor.uint16lsb() ) # mounts since last fsck
        attributes.put( 'max-mnt-count'        , cursor.uint16lsb() ) # max mounts w/o fsck
        
        attributes.put( 'magic'                , cursor.read( 2 )   )
        
        # 0x0001 - cleanly unmounted
        # 0x0002 - remount readonly
        # 0x0004 - orphans being recovered
        attributes.put( 'state'                , cursor.uint16lsb() )
        
        # behavior when detecting errors:
        #   1:continue 2:remount readonly 3:panic
        attributes.put( 'errors'               , cursor.uint16lsb() )
        
        attributes.put( 'minor-rev-level'      , cursor.uint16lsb() )
        
        attributes.put( 'last-check'           , cursor.uint32lsb() )
        attributes.put( 'check-interval'       , cursor.uint32lsb() )
        
        # 0:linux 1:hurd 2:masix 3:freebsd 4:lites
        attributes.put( 'creator-os'           , cursor.uint32lsb() )
        
        # 0:good old rev 1:dynamic rev
        attributes.put( 'rev-level'            , cursor.uint32lsb() )
        
        # default uid/gid for reserved blocks
        attributes.put( 'def-resuid'           , cursor.uint16lsb() )
        attributes.put( 'def-resgid'           , cursor.uint16lsb() )
        
        #
        # if not EXT4_DYNAMIC_REV, we return early here!
        # 
        if attributes.get( 'rev-level' ) == 0:
            return attributes
        
        attributes.put( 'first-ino'            , cursor.uint32lsb() )
        attributes.put( 'inode-size'           , cursor.uint16lsb() )
        attributes.put( 'block-group-nr'       , cursor.uint16lsb() )
        
        attributes.put( 'feature-compat'       , self._read_feature_compat( cursor ) )
        attributes.put( 'feature-incompat'     , self._read_feature_incompat( cursor ) )
        attributes.put( 'features-ro'          , self._read_feature_ro( cursor ) )
        
        attributes.put( 'uuid'                 , cursor.uuid() )
        attributes.put( 'volume-name'          , cursor.clipped( 16 ) )
        attributes.put( 'last-mounted'         , cursor.clipped( 64 ) )
        
        # unused in linux?
        attributes.put(
            'algorithm-used-bitmap',
            cursor.uint32lsb()
            )
        
        attributes.put( 'prealloc-blocks'     , cursor.uint8()     )
        attributes.put( 'prealloc-dir-blocks' , cursor.uint8()     )
        attributes.put( 'reserved-gdt-blocks' , cursor.uint16lsb() )
        
        attributes.put( 'journal-uuid'        , cursor.uuid()      )
        attributes.put( 'journal-inum'        , cursor.uint32lsb() )
        attributes.put( 'journal-dev'         , cursor.uint32lsb() )
        
        # start of list of orphans to delete
        attributes.put( 'last-orphan'         , cursor.uint32lsb() )
        
        attributes.put( 'hash-seed'           , [ cursor.uint32lsb() for _ in xrange( 4 ) ])
        
        # 0:legacy 1:half-md4 2:tea 3:legacy/unsigned 4:half-md4/unsigned 5:tea/unsigned
        attributes.put( 'hash-version'        , cursor.uint8() )
        
        attributes.put( 'jnl-backup-type'     , cursor.uint8() )
        
        attributes.put( 'desc-size'           , cursor.uint16lsb() )
        
        attributes.put( 'default-mount-opts'  , self._read_default_mount_options( cursor ) )
        
        # I see garbage in the unused mount opts from my test FS. weird.
        
        attributes.put( 'first-meta-bg'       , cursor.uint32lsb() )
        attributes.put( 'mkfs-time'           , cursor.uint32lsb() )
        
        attributes.put( 'jnl-blocks'          , [ cursor.uint32lsb() for _ in xrange( 17 ) ] )
        
        # hi-bits for values stored here are used if incompat-64bit
        
        attributes.put( 'blocks-count-hi'     , cursor.uint32lsb() )
        attributes.put( 'r-blocks-count-hi'   , cursor.uint32lsb() )
        attributes.put( 'free-blocks-count-hi', cursor.uint32lsb() )
        
        attributes.put( 'min-extra-isize'     , cursor.uint16lsb() )
        attributes.put( 'want-extra-isize'    , cursor.uint16lsb() )
        
        attributes.put( 'flags'               , self._read_superblock_flags( cursor ) )
        
        attributes.put( 'raid-stride'         , cursor.uint16lsb() )
        attributes.put( 'mmp-interval'        , cursor.uint16lsb() )
        attributes.put( 'mmp-block'           , cursor.uint64lsb() )
        
        attributes.put( 'raid-stripe-width'   , cursor.uint32lsb() )
        attributes.put( 'log-groups-per-flex' , cursor.uint8() )
        
        # 1:crc32c -- only valid value
        attributes.put( 'checksum-type'       , cursor.uint8() )
        
        attributes.put( 'reserved-pad'        , cursor.read( 2 ) )
        
        attributes.put( 'kbytes-written'      , cursor.uint64lsb() )
        
        attributes.put( 'snapshot-inum'       , cursor.uint32lsb() )
        attributes.put( 'snapshot-id'         , cursor.uint32lsb() )
        attributes.put(
            'snapshot-r-block-count',
            cursor.uint64lsb()      ,
            )
        attributes.put( 'snapshot-list'       , cursor.uint32lsb() )
        
        attributes.put( 'errors-count'        , cursor.uint32lsb() )
        attributes.put( 'first-error-time'    , cursor.uint32lsb() )
        attributes.put( 'first-error-ino'     , cursor.uint32lsb() )
        attributes.put( 'first-error-block'   , cursor.uint64lsb() )
        attributes.put( 'first-error-func'    , cursor.clipped( 32 ) )
        attributes.put( 'first-error-line'    , cursor.uint32lsb() )
        
        attributes.put( 'last-error-time'     , cursor.uint32lsb() )
        attributes.put( 'last-error-ino'      , cursor.uint32lsb() )
        attributes.put( 'last-error-line'     , cursor.uint32lsb() )
        attributes.put( 'last-error-block'    , cursor.uint64lsb() )
        attributes.put( 'last-error-func'     , cursor.clipped( 32 ) )
        
        attributes.put( 'mount-opts'          , cursor.clipped( 64 ) )
        
        attributes.put( 'usr-quota-inum'      , cursor.uint32lsb() )
        attributes.put( 'grp-quota-inum'      , cursor.uint32lsb() )
        
        attributes.put( 'overhead-blocks'     , cursor.uint32lsb() )
        
        attributes.put( 'backup-bgs'          , [ cursor.uint32lsb() for _ in xrange( 2 ) ])
        
        # upto 4 algos can be active at a time
        # why 4 32 ints and not flags I don't know
        # 0:invalid 1:aes-256-xts 2:aes-256-gcm 3:aes-256-cbc
        attributes.put( 'encrypt-algos'       , [ cursor.uint32lsb() for _ in xrange( 4 ) ])
        
        # reserved
        cursor.skip( 105 )
        
        attributes.put( 'checksum'            , cursor.uint32lsb() )
        
        return attributes
        
    def _check_flags( self, flags, value ):
        attributes    = Attributes()
        
        for flagName, flagValue in flags:
            # this lets it work with multi-flag values
            active = ( flagValue & value ) == flagValue
            attributes.put( flagName, active )
            if active:
                value -= flagValue
        
        attributes.put( '::unknown', value )
        return attributes
    
    def _read_feature_compat( self, cursor ):
        # an implementation can safely read and write regardless of support for these features
        features = [
            ( 'compat-dir-prealloc'  , 0x001 ) ,
            ( 'compat-imagic-inodes' , 0x002 ) ,
            ( 'compat-has-journal'   , 0x004 ) ,
            ( 'compat-ext-attr'      , 0x008 ) ,
            ( 'compat-resize-inode'  , 0x010 ) ,
            ( 'compat-dir-index'     , 0x020 ) ,
            ( 'compat-lazy-bg'       , 0x040 ) ,
            ( 'compat-exclude-inode' , 0x080 ) ,
            ( 'compat-exclude-bitmap', 0x100 ) ,
            ( 'compat-sparse-super2' , 0x200 ) ,
            ]
        return self._check_flags( features, cursor.uint32lsb() )
        
    def _read_feature_incompat( self, cursor ):
        # an implementation should stop immediately if it doesn't support one of these features
        features = [
            ( 'incompat-compression'     , 0x00001 ) ,
            ( 'incompat-filetype'        , 0x00002 ) ,
            ( 'incompat-recover'         , 0x00004 ) ,
            ( 'incompat-journal-dev'     , 0x00008 ) ,
            ( 'incompat-meta-bg'         , 0x00010 ) ,
            ( 'incompat-extents'         , 0x00040 ) ,
            ( 'incompat-64bit'           , 0x00080 ) ,
            ( 'incompat-mmp'             , 0x00100 ) ,
            ( 'incompat-flex-bg'         , 0x00200 ) ,
            ( 'incompat-ea-node'         , 0x00400 ) ,
            ( 'incompat-dir-data'        , 0x01000 ) ,
            ( 'incompat-bg-use-meta-csum', 0x02000 ) ,
            ( 'incompat-largedir'        , 0x04000 ) ,
            ( 'incompat-inline-data'     , 0x08000 ) ,
            ( 'incompat-encrypt'         , 0x10000 ) ,
            ]
        return self._check_flags( features, cursor.uint32lsb() )
    
    def _read_feature_ro( self, cursor ):
        # an implementation may only read if it doesn't support one of these features
        features = [
            ( 'ro-sparse-superblocks', 0x0001 ) ,
            ( 'ro-large-file'        , 0x0002 ) ,
            ( 'ro-btree-dir'         , 0x0004 ) ,
            ( 'ro-huge-file'         , 0x0008 ) ,
            ( 'ro-gdt-csum'          , 0x0010 ) ,
            ( 'ro-dir-nlink'         , 0x0020 ) ,
            ( 'ro-extra-isize'       , 0x0040 ) ,
            ( 'ro-has-snapshot'      , 0x0080 ) ,
            ( 'ro-quota'             , 0x0100 ) ,
            ( 'ro-bigalloc'          , 0x0200 ) ,
            ( 'ro-metadata-csum'     , 0x0400 ) ,
            ( 'ro-replica'           , 0x0800 ) ,
            ( 'ro-readonly'          , 0x1000 ) ,
            ]
        return self._check_flags( features, cursor.uint32lsb() )
    
    def _read_default_mount_options( self, cursor ):
        options = [
            ( 'defm-debug'         , 0x0001 ) ,
            ( 'defm-bsdgroups'     , 0x0002 ) ,
            ( 'defm-xattr-user'    , 0x0004 ) ,
            ( 'defm-acl'           , 0x0008 ) ,
            ( 'defm-uid16'         , 0x0010 ) ,
            ( 'defm-jmode-data'    , 0x0020 ) ,
            ( 'defm-jmode-ordered' , 0x0040 ) ,
            # defm-jmode-wback 0x60 fixed up after regular processing below
            ( 'defm-nobarrier'     , 0x0100 ) ,
            ( 'defm-block-validity', 0x0200 ) ,
            ( 'defm-discard'       , 0x0400 ) ,
            ( 'defm-nodelalloc'    , 0x0800 ) ,
            ]
        
        flags = self._check_flags( options, cursor.uint32lsb() )
        
        # fixup this, which is active when both other features are
        # it also bears two macros to define it in the original source
        flags.put( 'defm-jmode'      , flags.get( 'defm-jmode-data' ) and flags.get( 'defm-jmode-ordered' ) )
        flags.put( 'defm-jmode-wback', flags.get( 'defm-jmode-data' ) and flags.get( 'defm-jmode-ordered' ) )
        
        return flags
    
    def _read_superblock_flags( self, cursor ):
        flags = [
            ( 'signed-directory-hash-in-use'  , 0x01 ),
            ( 'unsigned-directory-hash-in-use', 0x02 ),
            ( 'testing-development-code'      , 0x04 ),
            ]
        
        return self._check_flags( flags, cursor.uint32lsb() )
    
    # # # # # # # # # # # # # # # # # #
    # these read functions expect the superblock to already be in place
    # 
    
    def _read_group_descriptor( self, cursor ):
        attributes = Attributes()
        start      = cursor.tell()
        
        descSize = self._get_desc_size()
        
        attributes.put( 'block-bitmap-lo'     , cursor.uint32lsb() )
        attributes.put( 'inode-bitmap-lo'     , cursor.uint32lsb() )
        attributes.put( 'inode-table-lo'      , cursor.uint32lsb() )
        attributes.put( 'free-blocks-count-lo', cursor.uint16lsb() )
        attributes.put( 'free-inodes-count-lo', cursor.uint16lsb() )
        attributes.put( 'used-dirs-count-lo'  , cursor.uint16lsb() )
        attributes.put( 'flags'               , self._read_group_descriptor_flags( cursor ) )
        attributes.put( 'exclude-bitmap-lo'   , cursor.uint32lsb() )
        attributes.put( 'block-bitmap-csum-lo', cursor.uint16lsb() )
        attributes.put( 'inode-bitmap-csum-lo', cursor.uint16lsb() )
        attributes.put( 'itable-unused-lo'    , cursor.uint16lsb() )
        attributes.put( 'checksum'            , cursor.uint16lsb() )
        
        flag64Bit = self._superblock.get( 'feature-incompat' ).get( 'incompat-64bit' )
        
        DEBUG_EXT( '## descSize:%s > 32 && flag64Bit:%s = gather-64-bit-fields:%s' % (
                repr( descSize  ) ,
                repr( flag64Bit ) ,
                repr( bool( flag64Bit and ( descSize > 32 ) ) ) ,
                ))
        
        if flag64Bit and ( descSize > 32 ):
            attributes.put( 'block-bitmap-hi'     , cursor.uint32lsb() )
            attributes.put( 'inode-bitmap-hi'     , cursor.uint32lsb() )
            attributes.put( 'inode-table-hi'      , cursor.uint32lsb() )
            attributes.put( 'free-blocks-count-hi', cursor.uint16lsb() )
            attributes.put( 'free-inodes-count-hi', cursor.uint16lsb() )
            attributes.put( 'used-dirs-count-hi'  , cursor.uint16lsb() )
            attributes.put( 'itable-unused-hi'    , cursor.uint16lsb() )
            attributes.put( 'exclude-bitmap-hi'   , cursor.uint32lsb() )
            attributes.put( 'block-bitmap-csum-hi', cursor.uint16lsb() )
            attributes.put( 'inode-bitmap-csum-hi', cursor.uint16lsb() )
            
            # only seek if descSize is set, otherwise let the 32bits taken stick
            cursor.seek( start + descSize )
        
        return attributes
    
    def _read_group_descriptor_flags( self, cursor ):
        flags = [
            ( 'inode-uninit', 0x1 ) ,
            ( 'block-uninit', 0x2 ) ,
            ( 'inode-zeroed', 0x4 ) ,
            ]
        
        return self._check_flags( flags, cursor.uint16lsb() )
    
    def _read_inode_descriptor( self, cursor ):
        attributes = Attributes()
        
        inodeDescriptorStart = cursor.tell()
        
        attributes.put( 'mode'               , self._read_inode_descriptor_mode( cursor ) )
        attributes.put( 'uid'                , cursor.uint16lsb() )
        attributes.put( 'size-lo'            , cursor.uint32lsb() )
        attributes.put( 'atime'              , cursor.uint32lsb() )
        attributes.put( 'ctime'              , cursor.uint32lsb() )
        attributes.put( 'mtime'              , cursor.uint32lsb() )
        attributes.put( 'dtime'              , cursor.uint32lsb() )
        attributes.put( 'gid'                , cursor.uint16lsb() )
        attributes.put( 'links-count'        , cursor.uint16lsb() )
        attributes.put( 'blocks-lo'          , cursor.uint32lsb() )
        attributes.put( 'flags'              , self._read_inode_descriptor_flags( cursor ) )
        
        attributes.put( 'os-specific'        , cursor.uint32lsb() )
        
        attributes.put( 'block-map'          , cursor.read( 60 ) )
        
        # file version for nfs
        attributes.put( 'generation'         , cursor.uint32lsb() )
        
        attributes.put( 'file-acl-lo'        , cursor.uint32lsb() )
        attributes.put( 'dir-acl-or-size-hi' , cursor.uint32lsb() )
        attributes.put( 'obso-faddr'         , cursor.uint32lsb() )
        
        attributes.put( 'os-specific-2'      , self._read_inode_descriptor_os_specific_2( cursor ) )
        
        if attributes.get( 'flags' ).get( '::unknown' ) != 0:
            raise Exception( 'fill in the flags' )
        
        # 
        # if we're using the original revision for inode sizes, it's done at 128; early escape!
        # 
        if self._superblock.get( 'rev-level' ) == self._GOOD_OLD_INODE_SIZE:
            return attributes
        
        extraStart = cursor.tell()
        
        # continuous checks for end of inode data so all inodes need not be extended on filesystem upgrade
        # probably not needed till 64 bytes further in, but whatever
        
        attributes.put( 'extra-isize'   , cursor.uint16lsb() )
        
        extraISize = attributes.get( 'extra-isize' )
        
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'checksum-hi'   , cursor.uint16lsb() )
        
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'ctime-extra'   , cursor.uint32lsb() )
        
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'mtime-extra'   , cursor.uint32lsb() )
        
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'atime-extra'   , cursor.uint32lsb() )
        
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'crtime'        , cursor.uint32lsb() )
        
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'crtime-extra'  , cursor.uint32lsb() )
        
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'verion-hi'     , cursor.uint32lsb() )
        
        # more future bits here
        
        # if it gets here, there's unknown junk at the end that should be known, note it and leave
        if cursor.tell() - extraStart < extraISize:
            attributes.put( 'remaining-isize' , cursor.read( 
                    attributes.get( 'extra-isize' ) - ( cursor.tell() - extraStart )
                    ))
        
        # print 'AT tell:%s start:%s thus:%s extra-isize:%s inode-size:%s' % (
        #     str( cursor.tell() ) ,
        #     str( inodeDescriptorStart ),
        #     str( cursor.tell() - inodeDescriptorStart ),
        #     str( attributes.get( 'extra-isize' ) ) ,
        #     str( self._get_inode_size() ) ,
        #     )
        
        # skip any empty space after the extra isize chunk and before the next inode
        # ( not that we currently walk the inode entries, but this will make it not break if we do )
        cursor.seek( inodeDescriptorStart + self._get_inode_size() )
        
        # print 'END', cursor.tell()
        
        return attributes
    
    def _read_inode_descriptor_mode( self, cursor ):
        
        modes = [
            ( 'ixoth',   0x1 ) ,
            ( 'iwoth',   0x2 ) ,
            ( 'iroth',   0x4 ) ,
            
            ( 'ixgrp',   0x8 ) ,
            ( 'iwgrp',  0x10 ) ,
            ( 'irgrp',  0x20 ) ,
            
            ( 'ixusr',  0x40 ) ,
            ( 'iwusr',  0x80 ) ,
            ( 'irusr', 0x100 ) ,
            
            ( 'isvtx', 0x200 ) ,
            ( 'isgid', 0x400 ) ,
            ( 'isuid', 0x800 ) ,
            
            ]
        
        checked = self._check_flags( modes, cursor.uint16lsb() )
        
        # the following are mutually exclusive
        # 
        fileTypes = [
            ( 'fifo' , 0x1000 ) , # fifo pipe
            ( 'chr'  , 0x2000 ) , # character device
            ( 'dir'  , 0x4000 ) , # directory
            ( 'blk'  , 0x6000 ) , # block device
            ( 'reg'  , 0x8000 ) , # regular file
            ( 'lnk'  , 0xa000 ) , # symbolic link
            ( 'sock' , 0xc000 ) , # socket
            ]
        
        for fileType in fileTypes:
            match = fileType[1] == checked.get( '::unknown' )
            checked.put( fileType[0], match )
            if match:
                checked.put( '::unknown', 0 )
        
        return checked
    
    def _read_inode_descriptor_flags( self, cursor ):
        flags = [
            ( 'secrm-fl'           , 0x00000001 ) , # requires secure deletion ( unimplemented )
            ( 'unrm-fl'            , 0x00000002 ) , # preserve for undelete    ( unimplemented )
            ( 'compr-fl'           , 0x00000004 ) , # file is compressed       ( not really implemented )
            ( 'sync-fl'            , 0x00000008 ) , # all writes to file must be synchronous
            ( 'immutable-fl'       , 0x00000010 ) , # file is immutable
            ( 'append-fl'          , 0x00000020 ) , # append only file
            ( 'nodump-fl'          , 0x00000040 ) , # dump(1)? utility should not dump this file
            ( 'noatime-fl'         , 0x00000080 ) , # do not update access time
            ( 'dirty-fl'           , 0x00000100 ) , # dirty compressed file ( unused )
            ( 'comprblk-fl'        , 0x00000200 ) , # file has one or more compressed clusters ( unused )
            ( 'nocompr-fl'         , 0x00000400 ) , # do not compress ( unused )
            ( 'encrypt-fl'         , 0x00000800 ) , # encrypted inode; previously compr-fl compression error
            ( 'index-fl'           , 0x00001000 ) , # directory has hashed indexes
            ( 'imagic-fl'          , 0x00002000 ) , # afs magic directory
            ( 'journal-data-fl'    , 0x00004000 ) , # file data must be written through journal
            ( 'notail-fl'          , 0x00008000 ) , # fail tail should not be merged ( unused in ext4 )
            ( 'dirsync-fl'         , 0x00010000 ) , # all directory entry data should be written syncronously
            ( 'topdir-fl'          , 0x00020000 ) , # top of directory heirarchy
            ( 'huge-file-fl'       , 0x00040000 ) , # this is a huge file
            ( 'extents-fl'         , 0x00080000 ) , # this file uses extents
            #                        0x00100000     # unused?
            ( 'ea-inode-fl'        , 0x00200000 ) , # inode used for large extended attribute
            ( 'eofblocks-fl'       , 0x00400000 ) , # file has blocks allocated past eof
            ( 'snapfile-fl'        , 0x01000000 ) , # inode is a snapshot ( not in mainline implementation )
            ( 'snapfile-deleted-fl', 0x04000000 ) , # snapshot is being deleted ( not in mainline )
            ( 'snapfile-shrunk-fl' , 0x08000000 ) , # snapshot shrink has completed ( not in mainline )
            ( 'inline-data-fl'     , 0x10000000 ) , # inode has inline data
            ( 'reserved-fl'        , 0x80000000 ) , # reserved for ext4 library
            ]
        # todo: add aggregate flags?
        return self._check_flags( flags, cursor.uint32lsb() )
    
    def _read_inode_descriptor_os_specific_2( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'blocks-high'  , cursor.uint16lsb() )
        attributes.put( 'file-acl-high', cursor.uint16lsb() )
        attributes.put( 'uid-high'     , cursor.uint16lsb() )
        attributes.put( 'gid-high'     , cursor.uint16lsb() )
        attributes.put( 'checksum-lo'  , cursor.uint16lsb() )
        attributes.put( 'reserved'     , cursor.uint16lsb() )
        
        return attributes
    
    # # # # # # # # # # # # # # # # # #
    # functions for traversing the data
    # used by directories and files in this filesystem
    # 
    
    def _get_block_size( self ):
        # minimum block size is 1024 under this method of determination
        # 
        logBlockSize = self._superblock.get( 'log-block-size' )
        
        # alternately, 1024 << logBlockSize
        # 
        return 2 ** ( 10 + logBlockSize )
    
    def _get_inode_size( self ):
        if self._superblock.get( 'rev-level' ) == self._GOOD_OLD_INODE_SIZE:
            return 128
        else:
            return self._superblock.get( 'inode-size' )
        
    def _get_flag_64bit( self ):
        return self._superblock.get( 'feature-incompat' ).get( 'incompat-64bit' )
    
    def _get_desc_size( self ):
        # size of group descriptor in bytes
        if not self._get_flag_64bit():
            return 32
        
        descSize = self._superblock.get( 'desc-size' )
        
        if descSize >= 32:
            return descSize
        else:
            print 'BAD DESCRIPTOR SIZE SPECIFICATION IN EXT*? USING 32 INSTEAD OF %s' % str( descSize )
            return 32
    
    def _get_group_descriptor_table_offset( self ):
        blockSize = self._get_block_size()
        
        if blockSize == 1024:
            # skip the second block as well since it will contain the superblock
            return blockSize * 2
        else:
            # for all larger sizes, the superblock is in the first block
            # just use the second
            return blockSize
        
    def _get_group_descriptor( self, groupNo ):
        if groupNo < 0:
            raise Exception( 'attempt to find descriptor for impossible group no' )
        
        descSize = self._get_desc_size()
        cursor   = self._rado.cursor()
        
        cursor.seek( self._get_group_descriptor_table_offset() )
        cursor.skip( descSize * groupNo )
        
        DEBUG_EXT( 'GROUP DESCRIPTOR TABLE OFFSET', self._get_group_descriptor_table_offset() )
        DEBUG_EXT( 'READING GROUP DESCRIPTOR FROM', cursor.tell() )
        
        groupDescriptor = self._read_group_descriptor( cursor )
        
        DEBUG_EXT( 'GROUP DESCRIPTOR', groupNo )
        DEBUG_EXT( groupDescriptor )
        
        return groupDescriptor
    
    def _get_inode_descriptor( self, inodeNo ):
        
        if inodeNo <= 0:
            raise Exception(
                'impossible inodeNo generated while parsing ext* filesystem'
                )
        
        inodesPerGroup  = self._superblock.get( 'inodes-per-group' )
        inodeSize       = self._get_inode_size()
        
        groupNo         = ( inodeNo - 1 ) / inodesPerGroup
        inodeTableIndex = ( inodeNo - 1 ) % inodesPerGroup
        
        groupDescriptor = self._get_group_descriptor( groupNo )
        
        flag64Bit = self._get_flag_64bit()
        if flag64Bit:
            raise Exception(
                '64 bit inode table lookup unimplemented'
                )
        else:
            inodeTableBlock = groupDescriptor.get( 'inode-table-lo' )
        
        inodeTableBlockOffset   = inodeTableBlock * self._get_block_size()
        descriptorOffsetInTable = inodeTableIndex * self._get_inode_size()
        
        descriptorOffset = inodeTableBlockOffset + descriptorOffsetInTable
        
        cursor = self._rado.cursor()
        cursor.seek( descriptorOffset )
        inodeDescriptor = self._read_inode_descriptor( cursor )
        
        return inodeDescriptor
    

class FileSystem__Ext__InodeContents__BlockDevice():
    def __init__( self, fileSystemExt, inodeDescriptor ):
        self._fileSystemExt   = fileSystemExt
        self._inodeDescriptor = inodeDescriptor
        return
    
    def size( self ):
        size = self._inodeDescriptor.get( 'size-lo' )
        
        # if we are a regular file, we add the dir-acl-or-size-hi
        # if we're a directory, it's an unused acl structure that we ignore
        #
        if self._inodeDescriptor.get( 'mode' ).get( 'reg' ):
            size += self._inodeDescriptor.get( 'dir-acl-or-size-hi' ) << 32
        
        return size
    
    def block_size( self ):
        return self._fileSystemExt._get_block_size()
    
    @Common.memoize( 'ext-inode-contents-blocks' )
    def get_block( self, blockNo ):
        rawBlockNo       = self._get_raw_block_no(
            contentsBlockNo = blockNo ,
            )
        
        rawBlockPosition = rawBlockNo * self._fileSystemExt._get_block_size()
        
        blockSize = self.block_size()
        
        remainingData = max(
            self.size() - ( blockNo * blockSize ) ,
            0                                     ,
            )
        
        remainingDataInBlock = min(
            remainingData ,
            blockSize     ,
            )
        
        return RadoRado(
            self._fileSystemExt._rado     ,
            offset = rawBlockPosition     ,
            size   = remainingDataInBlock ,
            )
    
    def _get_raw_block_no( self, contentsBlockNo ):
        # converts contents block no to a raw block no
        
        mode = self._inodeDescriptor.get( 'mode' )
        
        if not ( mode.get( 'dir' ) or mode.get( 'reg' ) ):
            # if the file does not have regular contents, do not look for them
            # 
            raise Exception( 'can only currently get reg/dir contents' )
        
        if self._fileSystemExt._superblock.get( 'feature-incompat' ).get( 'incompat-inline-data' ):
            # I think if this feature exists, any file with <= 60 bytes will have it stuffed
            # directly in the inode i_block field, the 'block-map' attribute
            raise Exception( 'possibilities of inline data are not currently handled' )
        
        if self._inodeDescriptor.get( 'flags' ).get( 'extents-fl' ):
            return self._get_raw_block_no_via_extents( contentsBlockNo )
        
        else:
            return self._get_raw_block_no_via_indirect_blocks( contentsBlockNo )
        
    def _get_raw_block_no_via_extents( self, contentsBlockNo ):
        # to get to the given block entry, we have to walk all entries upto it
        
        cursor = RadoBlob( self._inodeDescriptor.get( 'block-map' ) ).cursor()
        
        currentExtentHeader = self._read_ext4_extent_header( cursor )
        
        if currentExtentHeader.get( 'depth' ) == 0:
            
            for _ in xrange( currentExtentHeader.get( 'entries' ) ):
                extent = self._read_ext4_extent( cursor )
                # print extent
                
                desiredBlockContainedInExtent = (
                    extent.get( 'block' ) 
                    <= contentsBlockNo 
                    <= ( extent.get( 'block' ) + extent.get( 'len' ) )
                    )
                
                if desiredBlockContainedInExtent:
                    blockEntryInExtent = contentsBlockNo - extent.get( 'block' )
                    # print 'BLOCK ENTRY IN EXTENT', blockEntryInExtent
                    
                    rawBlockNo = ( extent.get( 'start-hi' ) << 16 ) + extent.get( 'start-lo' )
                    # print 'RAW BLOCK NO', rawBlockNo
                    
                    return rawBlockNo + blockEntryInExtent
                
            raise Exception( 'what now?' )
        
        else:
            blocksToProcess = []
            raise Exception( 'no handling indirection yet' )
        
        # print 'FIRST EXTENT HEADER'
        # print firstExtentHeader
        
        raise Exception( 'raw extent' )
    
    def _read_ext4_extent_header( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'magic'     , cursor.read( 2 ) )
        attributes.put( 'entries'   , cursor.uint16lsb() )
        attributes.put( 'max'       , cursor.uint16lsb() )
        attributes.put( 'depth'     , cursor.uint16lsb() )
        attributes.put( 'generation', cursor.uint32lsb() )
        
        return attributes
    
    def _read_ext4_extent( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'block'   , cursor.uint32lsb() ) # which contents block this is
        attributes.put( 'len'     , cursor.uint16lsb() ) # how many blocks it covers
        attributes.put( 'start-hi', cursor.uint16lsb() ) # hi part of 64bit raw block no
        attributes.put( 'start-lo', cursor.uint32lsb() ) # lo part of 64bit raw block no
        
        return attributes
    
    def _get_raw_block_no_via_indirect_blocks( self, contentsBlockNo ):
        
        cursor = RadoBlob( self._inodeDescriptor.get( 'block-map' ) ).cursor()
        
        # print repr( self._inodeDescriptor.get( 'block-map' ) )
        
        directs = [ cursor.uint32lsb() for _ in xrange( 11 ) ]
        
        singleIndirect = cursor.uint32lsb()
        doubleIndirect = cursor.uint32lsb()
        tripleIndirect = cursor.uint32lsb()
        
        if contentsBlockNo < 11:
            
            rawBlockNo = directs[ contentsBlockNo ]
            
            if rawBlockNo == 0:
                raise Exception(
                    'requested contentsBlockNo with null rawBlockNo'
                    )
            
            return rawBlockNo
        
        else:
            raise Exception( 'indirect lookup unimplemented' )


class FileSystem__Ext__Directory():
    name = 'file-system--ext--directory'
    
    def __init__( self, fileSystemExt, inodeNo ):
        self._fileSystemExt = fileSystemExt
        self._inodeNo       = inodeNo
        return
    
    _EXT4_NAME_LEN   = 255                 # how long a directory entry filename is
    _EXT4_DIRENT_SIZE = 8 + _EXT4_NAME_LEN # how long a full directory entry is
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    def list( self ):
        inodeDescriptor = self._fileSystemExt._get_inode_descriptor( self._inodeNo )
        dirents         = self._get_directory_entries( inodeDescriptor )
        
        entries = []
        for dirent in dirents:
            if dirent.get( 'name' ) not in [ '.', '..' ]:
                entries.append(
                    ( dirent.get( 'name' ), dirent.get( 'file-type' ) )
                    )
        
        return entries
    
    def select( self, what ):
        inodeDescriptor = self._fileSystemExt._get_inode_descriptor( self._inodeNo )
        dirents         = self._get_directory_entries( inodeDescriptor )
        
        for dirent in dirents:
            if dirent.get( 'name' ) in [ '.', '..' ]:
                continue
            
            elif dirent.get( 'name' ) == what:
                
                if dirent.get( 'file-type' ) == 2:
                    return FileSystem__Ext__Directory(
                        fileSystemExt = self._fileSystemExt   ,
                        inodeNo       = dirent.get( 'inode' ) ,
                        )
                
                if dirent.get( 'file-type' ) == 1:
                    return FileSystem__Ext__RegularFile(
                        fileSystemExt = self._fileSystemExt   ,
                        inodeNo       = dirent.get( 'inode' ) ,
                        )
                
                raise Exception(
                    'opening ext inode type %s unimplemented' % str( dirent.get( 'file-type' ) )
                    )
    
    def _get_directory_entries( self, inodeDescriptor ):
        contentsRado = RadoBlock( FileSystem__Ext__InodeContents__BlockDevice(
                fileSystemExt   = self._fileSystemExt ,
                inodeDescriptor = inodeDescriptor     ,
                ))
        
        cursor = contentsRado.cursor()
        
        contentsSize = contentsRado.size()
        
        if inodeDescriptor.get( 'flags' ).get( 'index-fl' ):
            print 'directory hash trees not yet implemented ( is regular form preserved? )'
        
        entries = []
        
        while True:
            
            if cursor.tell() == contentsSize:
                # eof, we've read all the dirents
                # 
                break
            
            if cursor.tell() > contentsSize:
                # erm, something's fucky in the dirent structure
                # 
                print 'FYI : BAD DIRENT IN DIRECTORY'
                break
            
            # print 'READING dirent from %s of %s' % (
            #     str( cursor.tell() ) ,
            #     str( contentsSize  ) ,
            #     )
            
            previousPosition = cursor.tell()
            
            entry = self._read_directory_entry( cursor )
            
            # print 'C:%s P:%s D:%s RL:%s entry:%s' % (
            #     str( cursor.tell() ) ,
            #     str( previousPosition ),
            #     str( cursor.tell() - previousPosition ),
            #     str( entry.get( 'rec-len' ) ),
            #     entry.to_flat(),
            #     )
            
            if entry.get( 'rec-len' ) == 0:
                print 'FYI : BAD RECLEN IN DIRECTORY DIRENT'
                break
            
            cursor.seek( previousPosition + entry.get( 'rec-len' ) )
            
            if entry.get( 'inode' ) != 0:
                # inode 0 directories are unused
                # 
                entries.append( entry )
        
        return entries
    
    def _read_directory_entry( self, cursor ):
        # name-len was uint16lsb in the original, but the value was never >= 255
        # so the high bytes were stolen for a file-type field to avoid having to
        # load all inodes of all files to determine type
        # 
        attributes = Attributes()
        attributes.put( 'inode'     , cursor.uint32lsb() )
        attributes.put( 'rec-len'   , cursor.uint16lsb() )
        attributes.put( 'name-len'  , cursor.uint8() )
        attributes.put( 'file-type' , cursor.uint8() )
        attributes.put( 'name'      , cursor.read( attributes.get( 'name-len' ) ) )
        return attributes


class FileSystem__Ext__RegularFile():
    name = 'file-system--ext--regular-file'
    
    def __init__( self, fileSystemExt, inodeNo ):
        self._fileSystemExt = fileSystemExt
        self._inodeNo       = inodeNo
        return
    
    def is_listable( self ): return False
    def is_radoable( self ): return True
    
    def rado( self ):
        inodeDescriptor = self._fileSystemExt._get_inode_descriptor( self._inodeNo )
        return RadoBlock( FileSystem__Ext__InodeContents__BlockDevice(
                fileSystemExt   = self._fileSystemExt ,
                inodeDescriptor = inodeDescriptor     ,
                ))


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- Macintosh
#- 

# @model
# class DiskFormat__Macintosh():
#     name = 'disk-format--macintosh'
#     
#     @staticmethod
#     def matches( rado ):
#         cursor = rado.cursor()
#         
#         # cursor.skip( 512 )
#         # print repr( cursor.read( 1024 ) )
#         
#         return False
#     
#     def __init__( self, rado ):
#         self._rado = rado
#         return
#     
#     def is_listable( self ): return True
#     def is_radoable( self ): return False


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- Apple Partition Map Disk
#- 

@model
class DiskFormat__ApplePartitionMap__Disk():
    name = 'disk-format--apple-partition-map--disk'
    
    @staticmethod
    def matches( rado ):
        cursor      = rado.cursor()
        magicNumber = cursor.read( 2 )
        return magicNumber == 'PM'
    
    def __init__( self, rado ):
        self._rado = rado
        return
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    def list( self ):
        raise Exception( 'nope' )
    
    def select( self ):
        raise Exception( 'nope' )


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- stuff it archive ( apparently popular on mac ? )
#- 

@model
class Archive__Stuff_It():
    name = 'archive--stuff-it'
    
    @staticmethod
    def matches( rado ):
        cursor      = rado.cursor()
        magicNumber = cursor.read( 4 )
        
        return magicNumber in Archive__Stuff_It.MAGIC_NUMBERS
    
    MAGIC_NUMBERS = [ 'SIT!', 'ST46', 'ST50', 'ST60', 'ST65', 'STin', 'STi2', 'STi3', 'STi4' ]
    
    def __init__( self, rado ):
        self._rado = rado
        return


#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- stuff it 5 archive ( updated format )
#- 

@model
class Archive__Stuff_It_Five():
    name = 'archive--stuff-it-five'
    
    @staticmethod
    def matches( rado ):
        cursor = rado.cursor()
        magicString = cursor.read( 78 )
        magicNumber = cursor.read(  2 )
        
        # after the 1997- there is a 4 digit termination date we're ignoring because it changes
        # 
        return ( magicString.startswith( 'StuffIt (c)1997-' )
                 and
                 magicString.endswith( ' Aladdin Systems, Inc., http://www.aladdinsys.com/StuffIt/' )
                 and
                 magicNumber == '\x0d\x0a'
                 )
    
    def __init__( self, rado ):
        self._rado        = rado
        
        cursor = rado.cursor()
        cursor.seek( 84 )
        
        self._archiveSize            = cursor.uint32msb()
        self._firstEntryHeaderOffset = cursor.uint32msb()
        
        print 'archiveSize:%s firstEntryHeaderOffset:%s' % (
            repr( self._archiveSize            ) ,
            repr( self._firstEntryHeaderOffset ) ,
            )
        
        cursor.seek( self._firstEntryHeaderOffset )
        firstEntryHeader = self._read_first_entry_header( cursor )
        
        print firstEntryHeader
        
        return
    
    def is_listable( self ): return True
    def is_radoable( self ): return False
    
    def _read_first_entry_header( self, cursor ):
        attributes = Attributes()
        
        attributes.put( 'identifier'  , cursor.uint32msb() )
        attributes.put( 'version'     , cursor.uint8()     )
        attributes.put( 'unknown'     , cursor.read( 1 )   )
        attributes.put( 'header-size' , cursor.uint16msb() )
        attributes.put( 'unknown-2'   , cursor.read( 1 )   )
        
        # replace with proper read
        # whether the entry is a file or folder depends on bit 6 of this flag
        # if set, it is a folder, which has a different format we'll need to write out
        # 
        attributes.put( 'flags' , cursor.uint8()     )
        
        # uses old mac seconds since 1904 format
        # 
        attributes.put( 'create-date'       , cursor.uint32msb() )
        attributes.put( 'modification-date' , cursor.uint32msb() )
        
        attributes.put( 'offset-previous-entry'     , cursor.uint32msb() )
        attributes.put( 'offset-next-entry'         , cursor.uint32msb() )
        attributes.put( 'offset-parent-folder-entry', cursor.uint32msb() )
        
        attributes.put( 'filename-size' , cursor.uint16msb() )
        attributes.put( 'header-crc-16' , cursor.uint16msb() )
        
        attributes.put( 'data-fork-uncompressed-length', cursor.uint32msb() )
        attributes.put( 'data-fork-compressed-length'  , cursor.uint32msb() )
        attributes.put( 'data-fork-crc-16'             , cursor.uint16msb() )
        
        attributes.put( 'unknown-3' , cursor.read( 1 ) )
        
        attributes.put( 'data-fork-compression-method', cursor.uint8() )
        
        attributes.put( 'further-entries', 'ignored' )
        
        return attributes

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-
#- test block layout
#- 

@model
class Test__BlockLayout():
    name = 'test--block-layout'
    
    @staticmethod
    def matches( rado ):
        return False
    
    def __init__( self, rado ):
        self._rado = rado
        return
    
    def is_radoable( self ): return False
    def is_listable( self ): return True
    
    def list( self ):
        return [ ('block-0', 'first-block'), ('block-n', 'nth-block') ]
    
    def select( self, what ):
        _, no = what.split( '-' )
        
        cursor = self._rado.cursor()
        
        cursor.seek( int( no ) * 4096 )
        v = cursor.uint32lsb()
        
        print 'FOUND', v
