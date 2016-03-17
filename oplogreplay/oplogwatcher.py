import time
import logging

import pymongo
from pymongo.errors import AutoReconnect, OperationFailure, DuplicateKeyError
from bson.timestamp import Timestamp

class OplogWatcher(object):
    """ Watches operation logs over a single mongo connection.

    Polls the oplog.rs collection of a given mongo connection for new oplog
    entries, and calls process_op for each new entry.
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

    def __init__(self, connection, ts=None, poll_time=1.0, databases=None):
        if ts is not None and not isinstance(ts, Timestamp):
            raise ValueError('ts argument: expected %r, got %r' % \
                             (Timestamp, type(ts)))
        self.poll_time = poll_time
        self.connection = connection
        self.ts = ts
        self.databases = databases

        # Mark as running.
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
            logging.info('Watching oplogs with timestamp > %s' % self.ts)
        else:
            logging.info('Watching all oplogs')

        while self.running:
            query = { 'ts': {'$gt': self.ts} }

            try:
                logging.debug('Tailing over %r...' % query)
                cursor = oplog.find(query, tailable=True)
                # OplogReplay flag greatly improves scanning for ts performance.
                cursor.add_option(pymongo.cursor._QUERY_OPTIONS['oplog_replay'])

                while self.running:
                    for op in cursor:
                        self.process_op(op['ns'], op)
                    time.sleep(self.poll_time)
                    if not cursor.alive:
                        break
            except AutoReconnect, e:
                logging.warning(e)
                time.sleep(self.poll_time)
            except OperationFailure, e:
                logging.exception(e)
                time.sleep(self.poll_time)

    def stop(self):
        self.running = False

    def process_op(self, ns, raw):
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
        process = True

        # check the database option and possibly not process this op
        if op != 'n' and self.databases:
            try:
                db, collection = ns.split('.', 1)
            except ValueError:
                logging.error("Unable to parse ns: %r" % ns)
            else:
                if db not in self.databases:
                    process = False

        if process:
            # Compute the document id of the document that will be altered
            # (in case of insert, update or delete).
            docid = self.__get_id(raw)

            if op == 'i':
                self.insert(ns=ns, docid=docid, raw=raw)
            elif op == 'u':
                self.update(ns=ns, docid=docid, raw=raw)
            elif op == 'd':
                self.delete(ns=ns, docid=docid, raw=raw)
            elif op == 'c':
                self.command(ns=ns, raw=raw)
            elif op == 'db':
                self.db_declare(ns=ns, raw=raw)
            elif op == 'n':
                self.noop()
            else:
                logging.error("Unknown op: %r" % op)

        # Save timestamp of last processed oplog.
        self.ts = raw['ts']

    def insert(self, ns, docid, raw, **kw):
        pass

    def update(self, ns, docid, raw, **kw):
        pass

    def delete(self, ns, docid, raw, **kw):
        pass

    def command(self, ns, raw, **kw):
        pass

    def db_declare(self, ns, **kw):
        pass

    def noop(self):
        pass
