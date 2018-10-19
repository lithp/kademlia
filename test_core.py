import collections
import pytest
import typing

from core import *


def test_bucket_ranges():
    assert bucket_ranges(0) == (1, 1)
    assert bucket_ranges(1) == (2, 3)
    assert bucket_ranges(2) == (4, 7)
    assert bucket_ranges(3) == (8, 15)

def test_random_key_in_bucket():
    myid = ID(0b10000)
    bucket = 4
    two = [random_key_in_bucket(myid, bucket).value for _ in range(200)]
    for i in range(0, 15):
        # assert that we've returned everything in the requested bucket at least once
        assert i in two

def test_bucket_index_for():
    nodeid = 0b1000
    table = RoutingTable(2, nodeid)

    assert table._bucket_index_for(0b1001) == 0  # distance of 1 -> first bucket
    assert table._bucket_index_for(0b1010) == 1  # distance of 2 -> second bucket
    assert table._bucket_index_for(0b1011) == 1  # distance of 3 -> second bucket
    assert table._bucket_index_for(0b1100) == 2  # distance of 4 -> third bucket
    assert table._bucket_index_for(0b0000) == 3  # distance of 8 -> fourth bucket

    nodeid = 0
    table = RoutingTable(2, nodeid)

    assert table._bucket_index_for(2**160 - 1) == 159  # this belongs in the last bucket


def test_first_element_of_ordered_dict():
    dictionary = collections.OrderedDict()
    dictionary[10] = 1
    dictionary[20] = 2

    assert RoutingTable._first_element_of_ordered_dict(dictionary) == (10, 1)
    dictionary.move_to_end(10)
    assert RoutingTable._first_element_of_ordered_dict(dictionary) == (20, 2)


def test_i_centered_indexes():
    assert list(RoutingTable._i_centered_indexes(0, 1)) == [(0,)]
    assert list(RoutingTable._i_centered_indexes(3, 5)) == [(3,), (2, 4), (1,), (0,)]

    def flatten(tups: typing.Iterator[typing.Tuple[int, ...]]):
        return [subitem for item in tups for subitem in item]

    # it should return a permutation of range(length)
    assert sorted(flatten(RoutingTable._i_centered_indexes(73, 160))) == list(range(160))


def test_calling_node_seen_bumps_last_seen():
    mynodeid = 0b1000
    table = RoutingTable(2, mynodeid)

    one_id = 0b1010
    one = Node('localhost', 9000, one_id)

    table.node_seen(one)
    old_last_seen = table.last_seen_for(one_id)

    table.node_seen(one)
    new_last_seen = table.last_seen_for(one_id)

    assert old_last_seen < new_last_seen


def test_filling_bucket_triggers_exception():
    mynodeid = 0b1000
    table = RoutingTable(2, mynodeid)

    one_id = 0b1100
    one = Node('localhost', 1, one_id)

    two_id = 0b1101
    two = Node('localhost', 2, two_id)

    three_id = 0b1110
    three = Node('localhost', 3, three_id)

    table.node_seen(one)
    table.node_seen(two)

    with pytest.raises(NoRoomInBucket):
        table.node_seen(three)


def test_can_evict_from_bucket():
    mynodeid = 0b1000
    table = RoutingTable(2, mynodeid)

    one_id = 0b1100
    one = Node('localhost', 1, one_id)

    two_id = 0b1101
    two = Node('localhost', 2, two_id)

    three_id = 0b1110
    three = Node('localhost', 3, three_id)

    table.node_seen(one)
    table.node_seen(two)

    table.evict_node(one_id)
    table.node_seen(three)  # does not raise NoRoomInBucket

def test_closest():
    mynodeid = 0b10000
    table = RoutingTable(2, mynodeid)

    def mknode(nodeid):
        return Node('localhost', 1, nodeid)

    table.node_seen(mknode(0b10010))
    assert table._bucket_index_for(0b10010) == 1

    # if there are fewer than k entries, we return all of them!
    assert len(table.closest(0b111110100)) == 1

    table.node_seen(mknode(0b10100))
    assert table._bucket_index_for(0b10100) == 2

    table.node_seen(mknode(0b10101))
    assert table._bucket_index_for(0b10101) == 2

    table.node_seen(mknode(0b11000))
    assert table._bucket_index_for(0b11000) == 3

    table.node_seen(mknode(0b110000))
    assert table._bucket_index_for(0b110000) == 5

    # bucket 1 isn't full, so we have to take a node from bucket 2 as well
    nodes = table.closest(0b10010)
    assert [node.nodeid for node in nodes] == [0b10010, 0b10100]

    # bucket 2 _is_ full, so we return all the elements from it
    nodes = table.closest(0b10100)
    assert [node.nodeid for node in nodes] == [0b10100, 0b10101]

    table.evict_node(0b10100)

    # bucket 2 is no longer full, so we look at the buckets nearby
    nodes = table.closest(0b10100)
    assert [node.nodeid for node in nodes] == [0b10101, 0b10010]

    # there are two buckets nearby and we return the node which is closest (by xor)
    nodes = table.closest(0b10101)
    assert node_distance(mynodeid, 0b10010) < node_distance(mynodeid, 0b11000)
    assert [node.nodeid for node in nodes] == [0b10101, 0b10010]

