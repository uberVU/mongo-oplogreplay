#!/usr/bin/python
import sys
import optparse

from lib.oplogreplay import OplogReplay

__version__ = 0.1

def parse_arguments():
    usage = '%prog [options] source destination'
    parser = optparse.OptionParser(usage=usage, version='%%prog %s' % __version__)

    parser.add_option('-r', '--replSet', action='store',
        dest='replicaset', help='ReplicaSet name')

    parser.add_option('-v', '--verbose', action='store_true',
        dest='verbose', help='increase verbosity')

    (options, args) = parser.parse_args()
    if len(args) != 2:
        sys.exit("Missing operands. Run with --help for more information.")

    options.source = args[0]
    options.dest = args[1]
    return options

def main():
    options = parse_arguments()
    oplogreplay = OplogReplay(options.source, options.dest,
                              replicaset=options.replicaset,
                              verbose=options.verbose)
    print 'Replaying oplogs...'
    oplogreplay.start()

if __name__ == '__main__':
    main()
