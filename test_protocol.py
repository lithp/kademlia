import asyncio
import pytest
import random

import protocol

def test_newnonce():
    nonce = protocol.newnonce()
    assert len(nonce) == 20


def test_read_write_nodeid():
    involve = lambda i: protocol.read_nodeid(protocol.write_nodeid(i))

    for i in range(100):
        assert i == involve(i)

    nodeid = 2**160 - 1
    assert protocol.write_nodeid(nodeid) == b'\xff'*20
    assert nodeid == involve(nodeid)


@pytest.mark.asyncio  # run the test inside an event loop so we don't have to make one
async def test_nonce_matching():
    'When you send a PING and get back a PONG with the same nonce the future is triggered'

    server = protocol.Server(k=2, mynodeid=100)
    await server.listen(3000)

    ping_message = protocol.create_ping()
    future = server.send(ping_message, ('localhost', 3001))

    # this is a lot of code just to send a UDP packet
    loop = asyncio.get_running_loop()
    transport, dgprotocol = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        local_addr = ('localhost', 3001)
    )

    await asyncio.sleep(0.1)
    assert not future.done()  # we have not yet sent the message

    pong_message = protocol.create_pong(ping_message)
    pong_message.nonce = b'garbage'
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    await asyncio.sleep(0.1)
    assert not future.done()  # we sent a message with the wrong nonce

    pong_message = protocol.create_pong(ping_message)
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    # we sent a conforming PONG, the future should now be set!
    try:
        await asyncio.wait_for(future, timeout=0.5)
    except asyncio.TimeoutError:
        assert False, 'the future was never set'

    assert future.done()

@pytest.mark.asyncio
async def test_incoming_messages_notify_routing_table():
    'When we receive a message we tell the routing table about it'

    # 1. Start an empty server
    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remoteid = 0b1001

    # 2. Send it a PONG
    loop = asyncio.get_running_loop()
    transport, dgprotocol = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        local_addr = ('localhost', 3001)
    )
    ping_message = protocol.create_ping()
    pong_message = protocol.create_pong(ping_message)
    pong_message.nonce = b'garbage'
    pong_message.sender.nodeid = protocol.write_nodeid(remoteid)  # it should be put into the first bucket
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    # 3. Look for the node in the routing table and see that it is good
    with pytest.raises(KeyError):
        server.table.last_seen_for(remoteid)

    await asyncio.sleep(0.1)
    assert server.table.last_seen_for(remoteid) is not None
