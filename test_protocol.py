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


class RecordingDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.messages = list()
    def datagram_received(self, data, addr):
        message = Message()
        message.ParseFromString(data)
        self.messages.append(message)


class MockServer:
    def __init__(self, transport, protocol):
        self.transport = transport
        self.protocol = protocol
    def send(self, addr, message):
        serialized = message.SerializeToString()
        self.transport.sendto(serialized, addr)
    @property
    def messages(self):
        return self.protocol.messages


async def startmockserver():
    'Binds a server to port 3001'
    loop = asyncio.get_running_loop()
    transport, proto = await loop.create_datagram_endpoint(
        RecordingDatagramProtocol, local_addr = ('localhost', 3001)
    )
    return MockServer(transport, proto)


@pytest.mark.asyncio  # run the test inside an event loop so we don't have to make one
async def test_nonce_matching():
    'When you send a PING and get back a PONG with the same nonce the future is triggered'
    mockserver = await startmockserver()

    local_node = core.Node(addr='localhost', port=3000, nodeid=100)

    server = protocol.Server(k=2, mynodeid=100)
    await server.listen(3000)

    ping_message = protocol.create_ping(local_node)
    future = server.send(ping_message, ('localhost', 3001))

    await asyncio.sleep(0.1)
    assert not future.done()  # we have not yet sent the message

    remote_node = core.Node(addr='localhost', port=3001, nodeid=110)

    pong_message = protocol.create_pong(remote_node, ping_message.nonce)
    pong_message.nonce = b'garbage'

    mockserver.send(('localhost', 3000), pong_message)

    await asyncio.sleep(0.1)
    assert not future.done()  # we sent a message with the wrong nonce

    pong_message = protocol.create_pong(remote_node, ping_message.nonce)

    mockserver.send(('localhost', 3000), pong_message)

    # we sent a conforming PONG, the future should now be set!
    try:
        await asyncio.wait_for(future, timeout=0.1)
    except asyncio.TimeoutError:
        assert False, 'the future was never set'

    assert future.done()


@pytest.mark.asyncio
async def test_incoming_messages_notify_routing_table():
    'When we receive a message we tell the routing table about it'
    mockserver = await startmockserver()

    # 1. Start an empty server
    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remoteid = 0b1001

    # 2. Send it a PONG
    remote_node = core.Node(addr='localhost', port=3001, nodeid=remoteid)
    pong_message = protocol.create_pong(remote_node, b'garbage')
    mockserver.send(('localhost', 3000), pong_message)

    # 3. Look for the node in the routing table and see that it is good
    with pytest.raises(KeyError):
        server.table.last_seen_for(remoteid)

    await asyncio.sleep(0.1)
    assert server.table.last_seen_for(remoteid) is not None


@pytest.mark.asyncio
async def test_server_send():
    '''
    Test that the server can send something which we can receive
    '''
    mockserver = await startmockserver()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    local_node = core.Node(addr='localhost', port=3000, nodeid=0b1000)
    remote_addr = ('localhost', 3001)
    remote_node = core.Node(addr='localhost', port=3001, nodeid=0b1001)

    # spin up a server
    assert len(mockserver.messages) == 0

    message = protocol.create_ping(local_node)
    server.send(message, remote_addr)

    await asyncio.sleep(0.1)
    assert len(mockserver.messages) == 1


@pytest.mark.asyncio
async def test_server_ping():
    '''
    When you call server.ping a PING packet is sent.
    It blocks until the PONG packet is received.
    '''
    mockserver = await startmockserver()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remote_addr = ('localhost', 3001)

    assert len(mockserver.messages) == 0

    coro = server.ping(remote_addr)
    task = asyncio.get_running_loop().create_task(coro)
    done, pending = await asyncio.wait({task}, timeout=0.2)
    assert task in pending

    assert len(mockserver.messages) == 1
    ping_message = mockserver.messages[0]

    remote_node = core.Node(addr='localhost', port=3001, nodeid=0b1001)
    pong_message = protocol.create_pong(remote_node, ping_message.nonce)
    mockserver.send(('localhost', 3000), pong_message)

    # now that we've sent a PONG it should be unblocked
    done, pending = await asyncio.wait({task}, timeout=0.2)
    assert task in done

@pytest.mark.asyncio
async def test_response_to_ping():
    'When you run a Server and send it a PING it responds with a PONG'
    mockserver = await startmockserver()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remote_node = core.Node(addr='localhost', port=3001, nodeid=0b1001)
    ping = protocol.create_ping(remote_node)
    mockserver.send(('localhost', 3000), ping)

    # we've sent a ping to the local node, once we give it a chance to run it should send
    # a pong back to us

    assert len(mockserver.messages) == 0
    await asyncio.sleep(0.1)
    assert len(mockserver.messages) == 1

    pong = mockserver.messages[0]
    assert pong.nonce == ping.nonce
    assert pong.sender.ip == 'localhost'
    assert pong.sender.port == 3000
    assert pong.sender.nodeid == protocol.write_nodeid(0b1000)

@pytest.mark.asyncio
async def test_responds_to_find_node():
    'When you run a Server and send it FIND_NODE it gives you all it has'
    mockserver = await startmockserver()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    # Ask it for some random node
    remote_node = core.Node(addr='localhost', port=3001, nodeid=0b1001)

    request = protocol.create_find_node(remote_node, targetnodeid=0b10000)
    mockserver.send(('localhost', 3000), request)

    # We should get a response back!
    assert len(mockserver.messages) == 0
    await asyncio.sleep(0.1)
    assert len(mockserver.messages) == 1

    # It should return a single element, us! (by sending it a message we added ourselves
    # to the routing table)
    response = mockserver.messages[0]
    assert response.nonce == request.nonce
    assert len(response.findNodeResponse.neighbors) == 1

    node = protocol.read_node(response.findNodeResponse.neighbors[0])
    assert node == remote_node

@pytest.mark.asyncio
async def test_responds_to_store():
    'When you run a Server and send it STORE it responds and also stores'
    mockserver = await startmockserver()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    # Ask it for some random node
    remote_node = core.Node(addr='localhost', port=3001, nodeid=0b1001)

    request = protocol.create_store(remote_node, key=b'abc', value=b'abc')
    mockserver.send(('localhost', 3000), request)

    # We should get a response back!
    assert len(mockserver.messages) == 0
    await asyncio.sleep(0.1)
    assert len(mockserver.messages) == 1

    # It should return a StoreResponse
    response = mockserver.messages[0]
    assert response.nonce == request.nonce
    assert response.HasField('storeResponse')

    # TODO: this is a horrible smell, put storage somewhere better!
    storage = server.protocol.storage
    assert b'abc' in storage
    assert storage[b'abc'] == b'abc'

@pytest.mark.asyncio
async def test_responds_to_find_value_when_no_value():
    'When you run a Server and send it just FIND_VALUE it gives you nearby nodes'
    mockserver = await startmockserver()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remote_node = core.Node(addr='localhost', port=3001, nodeid=0b1001)
    request = protocol.create_find_value(remote_node, key=b'abc')
    mockserver.send(('localhost', 3000), request)

    # We should get a response back!
    assert len(mockserver.messages) == 0
    await asyncio.sleep(0.1)
    assert len(mockserver.messages) == 1

    # It should return a FindNodeResponse
    response = mockserver.messages[0]
    assert response.nonce == request.nonce
    assert response.HasField('findNodeResponse')

@pytest.mark.asyncio
async def test_responds_to_find_value_when_has_value():
    'When you run a Server and send it STORE / FIND_VALUE it gives you the value'
    mockserver = await startmockserver()

    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remote_node = core.Node(addr='localhost', port=3001, nodeid=0b1001)

    request = protocol.create_store(remote_node, key=b'abc', value=b'abc')
    mockserver.send(('localhost', 3000), request)

    assert len(mockserver.messages) == 0
    await asyncio.sleep(0.1)
    assert len(mockserver.messages) == 1

    request = protocol.create_find_value(remote_node, key=b'abc')
    mockserver.send(('localhost', 3000), request)

    await asyncio.sleep(0.1)
    assert len(mockserver.messages) == 2

    # It should return a FoundValue
    response = mockserver.messages[1]
    assert response.nonce == request.nonce
    assert response.HasField('foundValue')
    assert response.foundValue.key == b'abc'
    assert response.foundValue.value == b'abc'

def test_parse_find_node_response():
    nodes = [core.Node(addr='localhost', port=i, nodeid=i) for i in range(5)]

    find_node_response = protocol.create_find_node_response(nodes[0], b'', nodes[0:])

    parsed_nodes = protocol.parse_find_node_response(find_node_response)

    assert parsed_nodes == nodes[0:]

@pytest.mark.asyncio
async def test_sending_find_node_response():
    loop = asyncio.get_running_loop()

    # start our server
    server = protocol.Server(k=2, mynodeid=0b1000)
    await server.listen(3000)

    remote = core.Node(addr='localhost', port=3001, nodeid=0b1001)
    nodes = [core.Node(addr='localhost', port=i, nodeid=i) for i in range(5)]

    # start the mock server
    class MyDatagramProtocol(asyncio.DatagramProtocol):
        def __init__(self):
            self.messages = list()
        def connection_made(self, transport):
            self.transport = transport
        def datagram_received(self, data, addr):
            message = Message()
            message.ParseFromString(data)
            self.messages.append(message)

            response = protocol.create_find_node_response(
                remote, message.nonce, nodes
            )
            serialized = response.SerializeToString()
            self.transport.sendto(serialized, ('localhost', 3000))
    transport, mdp = await loop.create_datagram_endpoint(
        MyDatagramProtocol, local_addr = ('localhost', 3001)
    )

    # send FIND_NODE and see that we correctly parse the response!
    targetnodeid = 0b1010
    result = await server.find_node(remote, targetnodeid)

    assert len(mdp.messages) == 1  # the mock server received a message
    assert mdp.messages[0].HasField('findNode')  # Server sent a FindNode message
    assert result == nodes  # Server parsed the nodes we gave it

