#!/usr/bin/env python

import argparse
import random

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from vtdb import topology
from zk import zkocc

# Constants and params
UNSHARDED = [keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)]

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--server', dest='server', default='localhost:15001')
parser.add_argument('--timeout', dest='timeout', type=float, default='10.0')
args = parser.parse_args()

vtgate_addrs = {"vt": [args.server]}

# Connect
conn = vtgatev2.connect(vtgate_addrs, args.timeout)

# Read topology
# This is a temporary work-around until the VTGate V2 client is topology-free.
topoconn = zkocc.ZkOccConnection(args.server, 'test', args.timeout)
topology.read_topology(topoconn)
topoconn.close()

# Insert something.
print('Inserting into master...')
cursor = conn.cursor('test_keyspace', 'master',
                     keyranges=UNSHARDED, writable=True)
cursor.begin()
cursor.execute(
    "INSERT INTO test_table (msg, replication_id) VALUES (%(msg)s, %(rep_id)s)",
    {'msg': 'V is for speed', 'rep_id': str(random.randint(10000,99999))})
cursor.commit()


# Read it back from the master.
print('Reading from master...')
cursor.execute('SELECT * FROM test_table', {})
for row in cursor.fetchall():
  print(row)

cursor.close()

# Read from a replica.
# Note that this may be behind master due to replication lag.
print('Reading from replica...')
cursor = conn.cursor('test_keyspace', 'replica',
                     keyranges=UNSHARDED)
cursor.execute('SELECT * FROM test_table', {})
for row in cursor.fetchall():
  print(row)
cursor.close()

# Clean up
conn.close()
