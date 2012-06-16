import unittest2 as unittest

import pymongo
import time
import random
import threading

from oplogreplay import OplogReplayer

SOURCE_HOST = '127.0.0.1:27017'
DEST_HOST = '127.0.0.1:27018'
TESTDB = 'testdb'

# Inherit from OplogReplayer to count number of processed_op methodcalls.
class CountingOplogReplayer(OplogReplayer):

    count = 0

    def process_op(self, ns, raw):
        OplogReplayer.process_op(self, ns, raw)
        CountingOplogReplayer.count += 1

class TestOplogReplayer(unittest.TestCase):
    """ TestCase for the OplogReplayer.

    Each test performs the following (see setUp and tearDown for more details):
      * delete test databases
      * start an OplogReplayer
      * perform some actions (inserts, etc.)
      * wait for the OplogReplayer to finish replaying ops
      * assertions
      * stop the OplogReplayer
    """

    @classmethod
    def setUpClass(cls):
        # Create connections to both test databases.
        cls.source = pymongo.Connection(SOURCE_HOST)
        cls.dest = pymongo.Connection(DEST_HOST)

    def _start_replay(self, **kwargs):
        # Stop the OplogReplayer before starting a new one.
        self._stop_replay()
        #if getattr(self, 'oplogreplayer', None):
        #    self._stop_replay()

        # Init & start OplogReplayer, in a separate thread.
        self.oplogreplayer = CountingOplogReplayer(
            SOURCE_HOST, DEST_HOST, poll_time=0.1, **kwargs)
        self.thread = threading.Thread(target=self.oplogreplayer.start)
        self.thread.start()

    def _stop_replay(self):
        # Stop OplogReplayer & join its thread.
        if getattr(self, 'oplogreplayer', None):
            self.oplogreplayer.stop()
        if getattr(self, 'thread', None):
            self.thread.join()
        # Delete oplogreplayer & thread.
        self.oplogreplayer = None
        self.thread = None

    def setUp(self):
        # Drop test databases.
        self.source.drop_database(TESTDB)
        self.dest.drop_database(TESTDB)
        self.dest.drop_database('oplogreplay')
        # Sleep a little to allow drop database operations to complete.
        time.sleep(0.05)

        # Remember Database objects.
        self.sourcedb = self.source.testdb
        self.destdb = self.dest.testdb

        # Stop replay, in case it was still running from a previous test.
        self._stop_replay()

        # Reset global counter & start OplogReplayer.
        CountingOplogReplayer.count = 0
        self._start_replay()

    def tearDown(self):
        self._stop_replay()

    def _synchronous_wait(self, target, timeout=3.0):
        """ Synchronously wait for the oplogreplay to finish.

        Waits until the oplog's retry_count hits target, but at most
        timeout seconds.
        """
        wait_until = time.time() + timeout
        while time.time() < wait_until:
            if CountingOplogReplayer.count == target:
                return
            time.sleep(0.05)
        # Synchronously waiting timed out - we should alert this.
        raise Exception('retry_count was only %s/%s after a %.2fsec wait' % \
                        (CountingOplogReplayer.count, target, timeout))

    def assertCollectionEqual(self, coll1, coll2):
        self.assertEqual(coll1.count(), coll2.count(),
                         msg='Collections have different count.')
        for obj1 in coll1.find():
            obj2 = coll2.find_one(obj1)
            self.assertEqual(obj1, obj2)

    def assertDatabaseEqual(self, db1, db2):
        self.assertListEqual(db1.collection_names(), db2.collection_names(),
                             msg='Databases have different collections.')
        for coll in db1.collection_names():
            self.assertCollectionEqual(db1[coll], db2[coll])

    def test_writes(self):
        self.sourcedb.testcoll.insert({'content': 'mycontent', 'nr': 1})
        self.sourcedb.testcoll.insert({'content': 'mycontent', 'nr': 2})
        self.sourcedb.testcoll.insert({'content': 'mycontent', 'nr': 3})
        self.sourcedb.testcoll.remove({'nr': 3})
        self.sourcedb.testcoll.insert({'content': 'mycontent', 'nr': 4})

        self.sourcedb.testcoll.insert({'content': 'mycontent', 'nr': 5})
        self.sourcedb.testcoll.insert({'content': '...', 'nr': 6})
        self.sourcedb.testcoll.update({'nr': 6}, {'$set': {'content': 'newContent'}})
        self.sourcedb.testcoll.update({'nr': 97}, {'$set': {'content': 'newContent'}})
        self.sourcedb.testcoll.update({'nr': 8}, {'$set': {'content': 'newContent'}}, upsert=True)

        self.sourcedb.testcoll.remove({'nr': 99})
        self.sourcedb.testcoll.remove({'nr': 3})
        self.sourcedb.testcoll.remove({'nr': 4})
        self.sourcedb.testcoll.insert({'content': 'new content', 'nr': 3})
        self.sourcedb.testcoll.insert({'content': 'new content', 'nr': 4})

        # Removes and updates that don't do anything will not hit the oplog:
        self._synchronous_wait(12)

        # Test that the 2 test databases are identical.
        self.assertDatabaseEqual(self.sourcedb, self.destdb)

    def _perform_bulk_inserts(self, nr=100):
        for i in xrange(nr):
            obj = { 'content': '%s' % random.random(),
                    'nr': random.randrange(100000) }
            self.sourcedb.testcoll.insert(obj)

    def test_bulk_inserts(self):
        self._perform_bulk_inserts(1000)

        self._synchronous_wait(1000)

        # Test that the 2 test databases are identical.
        self.assertDatabaseEqual(self.sourcedb, self.destdb)

    def test_discontinued_replay(self):
        self._perform_bulk_inserts(200)
        self._stop_replay()
        self._perform_bulk_inserts(150)
        self._start_replay()
        self._perform_bulk_inserts(100)

        self._synchronous_wait(450)

        # Test that the 2 test databases are identical.
        self.assertDatabaseEqual(self.sourcedb, self.destdb)

        # Test that no operation was replayed twice.
        self.assertEqual(CountingOplogReplayer.count, 450)

    def test_index_operations(self):
        # Create an index, then test that it was created on destionation.
        index = self.sourcedb.testidx.ensure_index('idxfield')
        self._synchronous_wait(1)
        self.assertIn(index, self.destdb.testidx.index_information())

        # Delete the index, and test that it was deleted from destination.
        self.sourcedb.testidx.drop_index(index)
        self._synchronous_wait(2)
        self.assertNotIn(index, self.destdb.testidx.index_information())

    def test_replay_indexes(self):
        # Create index1 on source + dest.
        index1 = self.sourcedb.testidx.ensure_index('idxfield1')

        # Restart OplogReplayer, without replaying indexes.
        self._start_replay(replay_indexes=False)

        # Create index2 on source only.
        index2 = self.sourcedb.testidx.ensure_index('idxfield2')
        # Delete index1 from source only.
        self.sourcedb.testidx.drop_index(index1)

        self._synchronous_wait(3)

        # Test indexes on source and destination.
        source_indexes = self.sourcedb.testidx.index_information()
        self.assertNotIn(index1, source_indexes)
        self.assertIn(index2, source_indexes)

        dest_indexes = self.destdb.testidx.index_information()
        self.assertIn(index1, dest_indexes)
        self.assertNotIn(index2, dest_indexes)

    def test_start_from_ts(self):
        self._stop_replay()

        # Should not be replayed:
        self.sourcedb.testcoll.insert({'content': 'mycontent', 'nr': 1})

        # Get last timestamp.
        obj = self.source.local.oplog.rs.find().sort('$natural', -1).limit(1)[0]
        lastts = obj['ts']

        # Should be replayed.
        self.sourcedb.testcoll.insert({'content': 'mycontent', 'nr': 1})

        self._start_replay(ts=lastts)

        self._synchronous_wait(1)

        self.assertEqual(self.destdb.testcoll.count(), 1)
