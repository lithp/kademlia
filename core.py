from __future__ import annotations  # allow forward references (PEP 563)

import collections
import datetime
import functools
import ipaddress
import itertools
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

class ID:
    def __init__(self, value: int = None):
        if value is None:
            value = new_node_id()
        self.value = value
    @classmethod
    def from_bytes(cls, as_bytes: bytes):
        as_int = int.from_bytes(as_bytes, byteorder='big')
        assert(as_int.bit_length() <= 160)
        return cls(as_int)
    def to_bytes(self) -> bytes:
        return self.valueto_bytes(20, byteorder='big')
    def distance(self, other) -> int:
        return self.value ^ other.value


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

    def first_occupied_bucket(self) -> int:
        'Returns the bucket containing our closest known neighbor'
        # TODO: write a test
        for index in range(160):
            bucket = self.buckets[index]
            if len(bucket) > 0:
                return index
        raise IndexError

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

    def closest(self, targetnodeid: int, n:int = None) -> typing.List[Node]:
        'Returns the k nodes we know of which are closest to key'
        if targetnodeid == self.nodeid:
            # _bucket_index_for will fail if we give it ourselves, we're not in any bucket
            return self.closest_to_me(n)

        bucket_index = self._bucket_index_for(targetnodeid)

        if n is None:
            n = self.k

        bucket_indexes = self._i_centered_indexes(bucket_index, 160)
        return self._closest_nodes(targetnodeid, n, bucket_indexes)

    def closest_to_me(self, n:int = None) -> typing.List[Node]:
        'Returns up to k nodes which are closest to self.nodeid'
        if n is None:
            n = self.k

        buckets_to_check = ([i] for i in range(160))
        return self._closest_nodes(self.nodeid, n, buckets_to_check)

    def _closest_nodes(self, targetnodeid: int, n: int, bucket_iterator):
        # TODO: this was a fun experiment but it's slower and harder to read than the
        #       imperative code, you should probably switch it back
        bucket_iterator: typing.Iterator[typing.List[int]]

        # bucket_iterator returns a sequence of buckets to check, each element in the
        # sequence will contain nodes which are further from the target than the previous
        # element. However, each element may contain multiple buckets, and those buckets
        # have no guaranteed ordering. So, the first step is to take each element and
        # merge the buckets inside them

        bucket_contents = lambda index: [val.node for val in self.buckets[index].values()]

        merged_buckets = (
            itertools.chain.from_iterable(
                (bucket_contents(index) for index in bucket_index_list)
            )
            for bucket_index_list in bucket_iterator
        )

        dist_from_target = lambda node: node_distance(targetnodeid, node.nodeid)
        sort_nodes = lambda nodes: sorted(nodes, key=dist_from_target)

        sorted_buckets = (sort_nodes(bucket) for bucket in merged_buckets)
        sorted_nodes = itertools.chain.from_iterable(sorted_buckets)

        return list(itertools.islice(sorted_nodes, n))

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

