import asyncio
import pytest


import kademlia
import protocol


@pytest.mark.asyncio
async def test_bootstrapping():
    node = kademlia.Node('localhost', 3000)
    await node.listen()

    remote = kademlia.Node('localhost', 3001)
    await remote.listen()

    far = kademlia.Node('localhost', 3002)
    await far.listen()

    remote.server.table.node_seen(far.node)

    await node.bootstrap('localhost', 3001)
    assert node.server.table.last_seen_for(far.node.nodeid) is not None


@pytest.mark.asyncio
async def test_store():
    node = kademlia.Node('localhost', 3000)
    await node.listen()

    first = protocol.Server(k=2, mynodeid=0b1000)
    second = protocol.Server(k=2, mynodeid=0b1001)
    third = protocol.Server(k=2, mynodeid=0b1010)

    await first.listen(3001)
    await second.listen(3002)
    await third.listen(3003)

    await node.bootstrap('localhost', 3001)

    first.table.node_seen(second.node)
    second.table.node_seen(third.node)

    await asyncio.wait_for(node.store_value(key=0b1010, value=b'hello'), timeout=0.1)

    storage = lambda server: server.protocol.storage

    assert 0b1010 in storage(third)
    assert 0b1010 in storage(second)
    assert 0b1010 not in storage(first)  # first is not in the first k peers


@pytest.mark.asyncio
async def test_value_lookup():
    node = kademlia.Node('localhost', 3000)
    await node.listen()

    first = protocol.Server(k=2, mynodeid=0b1000)
    second = protocol.Server(k=2, mynodeid=0b1001)
    third = protocol.Server(k=2, mynodeid=0b1010)

    await first.listen(3001)
    await second.listen(3002)
    await third.listen(3003)

    await node.bootstrap('localhost', 3001)

    first.table.node_seen(second.node)
    second.table.node_seen(third.node)

    third.protocol.storage[0b100] = b'hello'

    result = await asyncio.wait_for(node.find_value(0b100), timeout=0.1)
    assert result == b'hello'
