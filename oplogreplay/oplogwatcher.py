import time
import logging

import pymongo
from pymongo.errors import AutoReconnect, OperationFailure, DuplicateKeyError

class OplogWatcher(object):
    """ Watches operation logs over a single mongo connection.

    Longer class information.... - WRITE ME!
    """

    @staticmethod
    def __get_id(op):
        opid = None
        o2 = op.get('o2')
        if o2 is not None:
            opid = o2.get('_id')

        if opid is None:
            opid = op['o'].get('_id')

        return opid

    def __init__(self, connection, db=None, collection=None, poll_time=1.0, ts=None):
        if collection is not None:
            if db is None:
                raise ValueError('must specify db if you specify a collection')
            self._ns_filter = db + '.' + collection
        elif db is not None:
            self._ns_filter = db
        else:
            self._ns_filter = None

        self.poll_time = poll_time
        self.connection = connection
        self.ts = ts

        self.running = True

    def start(self):
        """ Starts the OplogWatcher. """
        oplog = self.connection.local['oplog.rs']

        if self.ts is None:
            cursor = oplog.find().sort('$natural', -1)
            obj = cursor[0]
            if obj:
                self.ts = obj['ts']
            else:
                # In case no oplogs are present.
                self.ts = None

        if self.ts:
            logging.info('Watching oplogs with timesteamp greater than %s' %
                         self.ts)
        else:
            logging.info('Watching all oplogs')

        while self.running:
            query = { 'ts': {'$gt': self.ts} }
            if self._ns_filter is not None:
                query['ns'] = self._ns_filter

            try:
                logging.debug('Tailing over %r...' % query)
                cursor = oplog.find(query, tailable=True)
                # OplogReplay flag greatly improves scanning for ts performance.
                cursor.add_option(pymongo.cursor._QUERY_OPTIONS['oplog_replay'])

                while self.running:
                    for op in cursor:
                        self.ts = op['ts']
                        opid = self.__get_id(op)
                        self.process_op(op['ns'], opid, op)
                    time.sleep(self.poll_time)
                    if not cursor.alive:
                        break
            except (AutoReconnect, OperationFailure):
                time.sleep(self.poll_time)

    def stop(self):
        self.running = False

    def process_op(self, ns, id, raw):
        """ Processes a single operation from the oplog.

        Performs a switch by raw['op']:
            "i" insert
            "u" update
            "d" delete
            "c" db cmd
            "db" declares presence of a database
            "n" no op
        """
        op = raw['op']
        if op == 'i':
            self.insert(ns=ns, id=id, obj=raw['o'], raw=raw)
        elif op == 'u':
            self.update(ns=ns, id=id, mod=raw['o'], raw=raw)
        elif op == 'd':
            self.delete(ns=ns, id=id, raw=raw)
        elif op == 'c':
            self.command(ns=ns, cmd=raw['o'], raw=raw)
        elif op == 'db':
            self.db_declare(ns=ns, raw=raw)
        elif op == 'n':
            self.noop()
        else:
            logging.error("Unknown op: %r" % op)

    def insert(self, ns, id, obj, raw, **kw):
        pass

    def update(self, ns, id, mod, raw, **kw):
        pass

    def delete(self, ns, id, raw, **kw):
        pass

    def command(self, ns, cmd, raw, **kw):
        pass

    def db_declare(self, ns, **kw):
        pass

    def noop(self):
        pass
