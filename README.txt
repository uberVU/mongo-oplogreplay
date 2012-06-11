===========
OplogReplay
===========

OplogReplay provides a simple tool that can replay MongoDB oplogs from one
cluster to another. Useful for admin operations, or to keep multiple copies
of the same data in different mongo clusters.

The included bin/oplogreplay script should cover most of the usecases::

    oplogreplay source:27017 destination:27017 --logpath=oplogreplay.log

(see oplogreplay --help for more details)

Installing
==========

Installing is as easy as::

    pip install oplogreplay

Advanced usage
==============

The package contains only two classes:
 * OplogWatcher
 * OplogReplayer

More advanced use-cases can be achieved by extending the OplogReplayer (e.g.:
you can choose to skip replaying deletes, or modify the operations before
replaying them into the destination cluster, etc.).
