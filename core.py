from __future__ import annotations  # allow forward references (PEP 563)

import collections
import datetime
import ipaddress
import random
import typing

Address = typing.Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

def new_node_id() -> int:
    'a random nodeid from [0, 2^160-1]'
    nodeid = random.getrandbits(160)
    assert nodeid.bit_length() <= 160
    return nodeid

def nodeid_as_bitstring(nodeid: int) -> str:
    return f"{nodeid:0160b}"

def node_distance(left: int, right: int) -> int:
    return left ^ right

def bucket_ranges(bucket_index: int) -> typing.Tuple[int, int]:
    'Returns the min and max value for a bucket'
    assert bucket_index >= 0
    assert bucket_index < 160

    return (
        2**bucket_index,
        (2**(bucket_index+1)) - 1
    )

def random_key_in_bucket(myid: int, bucket_index: int) -> int:
    'Returns a random key which belongs in the given bucket'
    distances = bucket_ranges(bucket_index) # everything in the bucket is this far from us
    distance = random.randrange(*distances)
    return myid ^ distance

class Node(typing.NamedTuple):
    addr: Address
    port: int
    nodeid: int

class RoutingEntry(typing.NamedTuple):
    node: Node
    last_seen: datetime.datetime

    def update_last_seen(self):
        now = datetime.datetime.utcnow()
        return self._replace(last_seen=now)

class NoRoomInBucket(Exception):
    '''
    Raised by RoutingTable.node_seen, indicates we should try to evict entry
    '''
    def __init__(self, entry: RoutingEntry):
        self.entry = entry

class RoutingTable:
    Bucket = typing.MutableMapping[int, RoutingEntry]  # Actually, an OrderedDict

    def __init__(self, k: int, mynodeid: int):
        self.k = k
        self.nodeid = mynodeid

        Buckets = typing.List[self.Bucket]
        self.buckets: Buckets = collections.defaultdict(collections.OrderedDict)

    def _bucket_index_for(self, nodeid: int) -> int:
        assert self.nodeid != nodeid
        assert nodeid >= 0
        assert nodeid <= (2**160 - 1)

        distance = node_distance(self.nodeid, nodeid)
        index = distance.bit_length() - 1

        assert index >= 0 and index < 160, index
        return index

    def _bucket_for(self, nodeid: int) -> RoutingTable.Bucket:
        bucket_index = self._bucket_index_for(nodeid)
        return self.buckets[bucket_index]
    
    def last_seen_for(self, nodeid: int) -> datetime.datetime:
        bucket_index = self._bucket_index_for(nodeid)
        bucket = self.buckets[bucket_index]
        entry = bucket[nodeid]  # may raise KeyError if nodeid is not known
        return entry.last_seen

    @staticmethod
    def _i_centered_indexes(i: int, length: int) -> typing.Iterator[int]:
        'Generates a permutation of range(length)'
        assert(length > 0)
        yield (i,)
        width = 1
        while True:
            to_return = tuple()
            if (i-width) >= 0:
                to_return += (i-width,)
            if (i+width) < length:
                to_return += (i+width,)

            if len(to_return):
                yield to_return
            else:
                break
            width += 1

    def closest(self, targetnodeid: int) -> typing.List[Node]:
        'Returns the k nodes we know of which are closest to key'
        bucket_index = self._bucket_index_for(targetnodeid)

        nodes = []
        for indexes in self._i_centered_indexes(bucket_index, len(self.buckets)):
            # indexes is a tuple of buckets to check next
            buckets = [self.buckets[index] for index in indexes]
            new_nodes = (entry.node for bucket in buckets for entry in bucket.values())
            closest = sorted(
                new_nodes,
                key=lambda node: node_distance(targetnodeid, node.nodeid)
            )
            nodes.extend(closest)

            if len(nodes) >= self.k:
                return nodes[:self.k]

        # we weren't able to find k nodes so we'll return every node we have
        return nodes

    def closest_to_me(self) -> typing.List[Node]:
        'Returns up to k nodes which are closest to self.nodeid'
        nodes = []
        for bucket in self.buckets:
            new_nodes = (entry.node for entry in bucket.values())
            closest = sorted(
                new_nodes,
                key=lambda node: node_distance(self.nodeid, node.nodeid)
            )
            nodes.extend(closest)

            if len(nodes) >= self.k:
                return nodes[:self.k]

    @staticmethod
    def _first_element_of_ordered_dict(dictionary: collections.OrderedDict):
        # There has to be a better way to peek() an OrderedDict
        assert(len(dictionary) > 0)
        return next(iter(dictionary.items()))

    def node_seen(self, node: Node):
        assert self.nodeid != node.nodeid, (self.nodeid, node.nodeid)

        bucket: collections.OrderedDict = self._bucket_for(node.nodeid)

        if node.nodeid in bucket:
            bucket[node.nodeid] = bucket[node.nodeid].update_last_seen()
            bucket.move_to_end(node.nodeid)
            return

        if len(bucket) < self.k:
            now = datetime.datetime.utcnow()
            entry = RoutingEntry(node=node, last_seen=now)
            bucket[node.nodeid] = entry
            return

        nodeid, routing_entry = self._first_element_of_ordered_dict(bucket)
        raise NoRoomInBucket(routing_entry)

    def evict_node(self, nodeid: int):
        'Removes this node from the routing table'
        bucket: collections.OrderedDict = self._bucket_for(nodeid)
        del bucket[nodeid]

