
import sys

import subprocess
from contextlib import contextmanager

import uu

def main():
    arguments = sys.argv[1:]
    
    if not len( arguments ):
        fail( 'you must supply the filename' )
        return
    
    targetFilename = arguments.pop( 0 )
    if not targetFilename or targetFilename[0] == '-':
        fail( 'given filename does not appear to be a file : %s' % repr( targetFilename ) )
        return
    
    with open( targetFilename ) as ff:
        print '-open %s' % repr( targetFilename )
        
        fileBlockDevice = uu.File__BlockDevice(
            name    = 'initial-file-blockdevice' ,
            fileobj = ff                         ,
            )
        
        rado = uu.RadoBlock( 
            name        = 'initial-file-rado-block' ,
            blockDevice = fileBlockDevice           ,
            )
        
        currentModel = uu.ModelUnknownBlob( rado )
        
        while arguments:
            
            argument = arguments.pop( 0 )
            
            if argument == '-ff':
                # fast forward
                # looks for a list with a _ entry
                # takes first option on each list till dead end, it hits it, or max depth ( 32 by default )
                target = arguments.pop( 0 )
                print argument, target
                
                while True:
                    if currentModel.is_listable():
                        options = currentModel.list()
                        
                        if not options:
                            fail( 'could not fast forward, encountered listable with no options' )
                        elif target in [ option[0] for option in options ]:
                            # success!
                            # 
                            print '-ff -select %s # success' % repr( target )
                            currentModel = currentModel.select( target )
                            if not currentModel:
                                fail( 'attempt to fast forward by selecting target somehow failed' )
                            else:
                                break
                        else:
                            print '-ff -select %s # first' % repr( options[0][0] )
                            currentModel = currentModel.select( options[0][0] )
                            if not currentModel:
                                fail( 'attempt to fast forward by selecting first option somehow failed' )
                            else:
                                continue
                    
                    if currentModel.is_radoable():
                        compatibleModel = uu.first_compatible_model( currentModel.rado() )
                        if not compatibleModel:
                            fail( 'could not fast forward, unknown binary type in chain' )
                        else:
                            print '-ff -assume %s' % repr( compatibleModel.name )
                            currentModel = compatibleModel( currentModel.rado() )
                            continue
                    
                    raise Exception( 'model is neither listable nor scanable' )
                
                continue
                
            if argument == '-list':
                print argument
                if currentModel.is_listable():
                    print repr( currentModel.list() )
                    continue
                else:
                    print '# model %s cannot list' % repr( currentModel.name )
            
            if argument == '-scan':
                print argument
                if currentModel.is_radoable():
                    hadany = False
                    for compatible, compatibleModel in uu.determine_compatible_models( currentModel.rado() ):
                        if compatible:
                            hadany = True
                            print '# == %s' % repr( compatibleModel.name )
                        else:
                            print '# != %s' % repr( compatibleModel.name )
                        
                    if not hadany:
                        print '# no matching models :('
                        
                    continue
                else:
                    fail( 'model is not scanable' )
            
            if argument == '-assume':
                print argument
                if not currentModel.is_radoable():
                    fail( 'current model is not radoable' )
                else:
                    compatibleModel = uu.first_compatible_model( currentModel.rado() )
                    if not compatibleModel:
                        fail( 'no compatible model to assume :(' )
                    else:
                        print '# assumed %s' % repr( compatibleModel.name )
                        currentModel = compatibleModel( currentModel.rado() )
                        continue
            
            if argument == '-as':
                target = arguments.pop( 0 )
                print argument, repr( target )
                currentModel = uu.model_by_name( target )( currentModel.rado() )
                print '# !!', target
                continue
            
            if argument == '-dump':
                print argument
                
                # try to dump the file contents to the terminal for review
                cursor = currentModel.rado().cursor()
                
                print '/--'
                print '| '
                for line in cursor.readlines():
                    print '| ', line.rstrip()
                print '| '
                print '\\--'
                continue
            
            if argument == '-hex':
                print argument
                
                cursor = currentModel.rado().cursor()
                
                def two( v ):
                    if len( v ) == 1: return '0' + v
                    return v
                
                while True:
                    grabbed = cursor.read( 16 )
                    if not grabbed:
                        break
                    
                    print ' '.join([
                        two( hex( ord( c ) ).split('x')[1] )
                        for c in grabbed[:8]
                    ]),
                    
                    print '',
                    
                    print ' '.join([
                        two( hex( ord( c ) ).split('x')[1] )
                        for c in grabbed[8:]
                    ]),
                    
                    print ' ',
                    
                    print (
                        '|'
                        + ''.join( [ c if c.isalnum() else '.' for c in grabbed[:8] ] )
                        + ' '
                        + ''.join( [ c if c.isalnum() else '.' for c in grabbed[8:] ] )
                        + '|'
                    )
                    
                continue
            
            if argument == '-copy':
                # onerous restrictions to avoid disaster, for now
                target = arguments.pop( 0 )
                print '-copy %s' % repr( target )
                if not target   : raise Exception( 'must have legit target' )
                if '/' in target: raise Exception( 'no / in target' )
                
                opener = None
                
                if target == '-':
                    print '# target - : using STDOUT'
                    
                    # keeps us from accidentally closing stdout when we're done pumping out file to it
                    @contextmanager
                    def opener():
                        yield sys.stdout
                    
                elif not target.startswith( 'COPY-' ):
                    raise Exception( 'target must begin COPY-' )
                
                else:
                    def opener():
                        return open( target, 'w' )
                
                with opener() as f:
                    cursor = currentModel.rado().cursor()
                    while True:
                        blob = cursor.read( 2048 )
                        if blob:
                            f.write( blob )
                        else:
                            break
                    continue
            
            if argument == '-magic':
                # pipe the data from the file into the file command to determine type by magic
                p = subprocess.Popen(
                    [ 'file', '-' ] ,
                    stdin  = subprocess.PIPE ,
                    stdout = subprocess.PIPE ,
                    stderr = subprocess.PIPE ,
                )
                
                currentRado = currentModel.rado()
                
                ( stdout, stderr ) = p.communicate(
                    currentRado.cursor().read( currentRado.size() )
                )
                
                if p.returncode != 0:
                    raise Exception( '"file" command failed with %s' % repr( stderr ) )
                
                else:
                    print '# -magic : output of "file -"'
                    print stdout
                
                continue
            
            # unknown command or child selector
            
            if argument and argument[0] == '-':
                if argument.startswith( '--' ):
                    fail( 'commands start with "-", not "--"' )
                    
                fail( 'unknown command %s' % repr( argument ) )
            
            if not argument:
                fail( 'empty argument' )
            
            if currentModel.is_listable():
                print '-select %s' % repr( argument )
                currentModel = currentModel.select( argument )
                if not currentModel:
                    fail( 'selection not found' )
                continue
            
            fail( 'impossible error' )
        
        print '# current model listable:%s scanable:%s' % (
            str( currentModel.is_listable() ) ,
            str( currentModel.is_radoable() ) ,
            )
            
        print '# done reads:%s' % (
            repr( fileBlockDevice.get_blocks_read() ) ,
            )

def fail( message ):
    print '# ! %s' % message
    sys.exit( 1 )

    
if __name__ == '__main__':
    main()
