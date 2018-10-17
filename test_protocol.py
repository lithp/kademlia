import asyncio
import logging
import pytest
import random

import core
import protocol

from protobuf.rpc_pb2 import Message


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

    local_node = core.Node(addr='localhost', port=3000, nodeid=100)

    server = protocol.Server(k=2, mynodeid=100)
    await server.listen(3000)

    ping_message = protocol.create_ping(local_node)
    future = server.send(ping_message, ('localhost', 3001))

    # this is a lot of code just to send a UDP packet
    loop = asyncio.get_running_loop()
    transport, dgprotocol = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        local_addr = ('localhost', 3001)
    )

    await asyncio.sleep(0.1)
    assert not future.done()  # we have not yet sent the message

    remote_node = core.Node(addr='localhost', port=3001, nodeid=110)

    pong_message = protocol.create_pong(remote_node, ping_message.nonce)
    pong_message.nonce = b'garbage'
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    await asyncio.sleep(0.1)
    assert not future.done()  # we sent a message with the wrong nonce

    pong_message = protocol.create_pong(remote_node, ping_message.nonce)
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
    remote_node = core.Node(addr='localhost', port=3001, nodeid=remoteid)
    pong_message = protocol.create_pong(remote_node, b'garbage')
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    # 3. Look for the node in the routing table and see that it is good
    with pytest.raises(KeyError):
        server.table.last_seen_for(remoteid)

    await asyncio.sleep(0.1)
    assert server.table.last_seen_for(remoteid) is not None

class RecordingDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.messages = list()
    def datagram_received(self, data, addr):
        message = Message()
        message.ParseFromString(data)
        self.messages.append(message)


@pytest.mark.asyncio
async def test_server_send():
    '''
    Test that the server can send something which we can receive
    '''
    loop = asyncio.get_running_loop()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    local_node = core.Node(addr='localhost', port=3000, nodeid=0b1000)
    remote_addr = ('localhost', 3002)
    remote_node = core.Node(addr='localhost', port=3002, nodeid=0b1001)

    # spin up a server
    transport, dgprotocol = await loop.create_datagram_endpoint(
        RecordingDatagramProtocol, local_addr = remote_addr
    )

    assert len(dgprotocol.messages) == 0

    message = protocol.create_ping(local_node)
    server.send(message, remote_addr)

    await asyncio.sleep(0.1)
    assert len(dgprotocol.messages) == 1


@pytest.mark.asyncio
async def test_server_ping():
    '''
    When you call server.ping a PING packet is sent.
    It blocks until the PONG packet is received.
    '''
    loop = asyncio.get_running_loop()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remote_addr = ('localhost', 3002)

    # spin up a server
    transport, dgprotocol = await loop.create_datagram_endpoint(
        RecordingDatagramProtocol, local_addr = remote_addr
    )

    assert len(dgprotocol.messages) == 0

    # I don't understand why, but for this test to pass we must create the task after
    # we create the datagram endpoint

    coro = server.ping(remote_addr)
    task = loop.create_task(coro)
    done, pending = await asyncio.wait({task}, timeout=0.2)
    assert task in pending

    assert len(dgprotocol.messages) == 1
    ping_message = dgprotocol.messages[0]

    remote_node = core.Node(addr='localhost', port=3002, nodeid=0b1001)
    pong_message = protocol.create_pong(remote_node, ping_message.nonce)
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    # now that we've sent a PONG it should be unblocked
    done, pending = await asyncio.wait({task}, timeout=0.2)
    assert task in done

@pytest.mark.asyncio
async def test_response_to_ping():
    'When you run a Server and send it a PING it responds with a PONG'
    loop = asyncio.get_running_loop()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remote_addr = ('localhost', 3002)
    transport, dgprotocol = await loop.create_datagram_endpoint(
        RecordingDatagramProtocol, local_addr = remote_addr
    )

    remote_node = core.Node(addr='localhost', port=3002, nodeid=0b1001)
    ping = protocol.create_ping(remote_node)
    serialized = ping.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    # we've sent a ping to the local node, once we give it a chance to run it should send
    # a pong back to us

    assert len(dgprotocol.messages) == 0
    await asyncio.sleep(0.1)
    assert len(dgprotocol.messages) == 1

@pytest.mark.asyncio
async def test_responds_to_find_node():
    pass

