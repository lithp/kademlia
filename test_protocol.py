import asyncio
import logging
import pytest
import random

import core
import messages
import protocol

from protobuf.rpc_pb2 import Message

ID = core.ID


class RecordingDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.messages = list()
        self.future = None
    def datagram_received(self, data, addr):
        message = Message()
        message.ParseFromString(data)
        self.messages.append(message)
        if self.future:
            self.future.set_result(message)
            self.future = None
    def next_message_future(self):
        self.future = asyncio.get_running_loop().create_future()
        return self.future


class MockServer:
    def __init__(self, transport, protocol, default_send_port):
        self.transport = transport
        self.protocol = protocol
        self.default_send_port = default_send_port
    def send(self, message, addr = None):
        if addr is None:
            addr = ('localhost', self.default_send_port)
        serialized = message.SerializeToString()
        self.transport.sendto(serialized, addr)
    @property
    def messages(self):
        return self.protocol.messages
    def next_message_future(self):
        return self.protocol.next_message_future()


async def startmockserver(default_send_port: int) -> MockServer:
    'Binds a server to port 3001'
    loop = asyncio.get_running_loop()
    transport, proto = await loop.create_datagram_endpoint(
        RecordingDatagramProtocol, local_addr = ('localhost', 3001)
    )
    return MockServer(transport, proto, default_send_port)


@pytest.mark.asyncio  # run the test inside an event loop so we don't have to make one
async def test_nonce_matching():
    'When you send a PING and get back a PONG with the same nonce the future is triggered'
    mockserver = await startmockserver(3000)

    local_node = core.Node(addr='localhost', port=3000, nodeid=ID(100))
    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(110))

    server = protocol.Server(mynodeid=ID(100))
    await server.listen('localhost', 3000)

    ping_message = messages.Ping()
    future = server.send(ping_message, remote_node)

    await asyncio.sleep(0.1)
    assert not future.done()  # we have not yet sent the message

    pong_message = messages.Pong(ping_message.nonce).finalize(remote_node)
    pong_message.nonce = b'garbage'
    mockserver.send(pong_message)

    await asyncio.sleep(0.1)
    assert not future.done()  # we sent a message with the wrong nonce

    pong_message = messages.Pong(ping_message.nonce).finalize(remote_node)
    mockserver.send(pong_message)

    # we sent a conforming PONG, the future should now be set!
    await asyncio.sleep(0.1)
    assert future.done()


@pytest.mark.asyncio
async def test_incoming_messages_notify_routing_table():
    'When we receive a message we tell the routing table about it'
    mockserver = await startmockserver(3000)

    # 1. Start an empty server
    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    remoteid = ID(0b1001)

    # 2. Send it a PONG
    remote_node = core.Node(addr='localhost', port=3001, nodeid=remoteid)
    pong_message = messages.Pong(b'garbage').finalize(remote_node)
    mockserver.send(pong_message)

    # 3. Look for the node in the routing table
    with pytest.raises(KeyError):
        server.table.last_seen_for(remoteid)
    await asyncio.sleep(0.1)
    assert server.table.last_seen_for(remoteid) is not None


@pytest.mark.asyncio
async def test_server_send():
    '''
    Test that the server can send something which we can receive
    '''
    mockserver = await startmockserver(3000)

    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    local_node = core.Node(addr='localhost', port=3000, nodeid=ID(0b1000))
    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))

    # spin up a server
    assert len(mockserver.messages) == 0

    future = mockserver.next_message_future()

    message = messages.Ping()
    server.send(message, remote_node)

    await future
    assert len(mockserver.messages) == 1


@pytest.mark.asyncio
async def test_server_ping():
    '''
    When you call server.ping a PING packet is sent.
    It blocks until the PONG packet is received.
    '''
    mockserver = await startmockserver(3000)

    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))

    assert len(mockserver.messages) == 0

    coro = server.ping('localhost', 3001)
    task = asyncio.get_running_loop().create_task(coro)
    done, pending = await asyncio.wait({task}, timeout=0.2)
    assert task in pending

    assert len(mockserver.messages) == 1
    ping_message = mockserver.messages[0]

    pong_message = messages.Pong(ping_message.nonce).finalize(remote_node)
    mockserver.send(pong_message)

    # now that we've sent a PONG it should be unblocked
    done, pending = await asyncio.wait({task}, timeout=0.2)
    assert task in done

@pytest.mark.asyncio
async def test_response_to_ping():
    'When you run a Server and send it a PING it responds with a PONG'
    mockserver = await startmockserver(3000)

    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    future = mockserver.next_message_future()

    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))
    ping = messages.Ping().finalize(remote_node)
    mockserver.send(ping)

    # we've sent a ping to the local node, once we give it a chance to run it should send
    # a pong back to us

    assert len(mockserver.messages) == 0
    await future
    assert len(mockserver.messages) == 1

    pong = mockserver.messages[0]
    assert pong.nonce == ping.nonce
    assert pong.sender.ip == 'localhost'
    assert pong.sender.port == 3000
    assert pong.sender.nodeid == ID(0b1000).to_bytes()

@pytest.mark.asyncio
async def test_responds_to_find_node():
    'When you run a Server and send it FIND_NODE it gives you all it has'
    mockserver = await startmockserver(3000)

    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    # Ask it for some random node
    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))
    request = messages.FindNode(ID(0b10000)).finalize(remote_node)
    mockserver.send(request)

    # We should get a response back!
    assert len(mockserver.messages) == 0
    await mockserver.next_message_future()
    assert len(mockserver.messages) == 1

    # It should return a single element, us! (by sending it a message we added ourselves
    # to the routing table)
    response = mockserver.messages[0]
    assert response.nonce == request.nonce
    assert len(response.findNodeResponse.neighbors) == 1

    node = messages.Message._parse_node(response.findNodeResponse.neighbors[0])
    assert node == remote_node

@pytest.mark.asyncio
async def test_responds_to_store():
    'When you run a Server and send it STORE it responds and also stores'
    mockserver = await startmockserver(3000)

    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    # Ask it for some random node
    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))

    request = messages.Store(key=ID(0b100), value=b'abc').finalize(remote_node)
    mockserver.send(request)

    # We should get a response back!
    assert len(mockserver.messages) == 0
    await mockserver.next_message_future()
    assert len(mockserver.messages) == 1

    # It should return a StoreResponse
    response = mockserver.messages[0]
    assert response.nonce == request.nonce
    assert response.HasField('storeResponse')

    assert 0b100 in server.storage
    assert server.storage[0b100] == b'abc'

@pytest.mark.asyncio
async def test_responds_to_find_value_when_no_value():
    'When you run a Server and send it just FIND_VALUE it gives you nearby nodes'
    mockserver = await startmockserver(3000)

    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))
    request = messages.FindValue(ID(0b100)).finalize(remote_node)
    mockserver.send(request)

    # We should get a response back!
    assert len(mockserver.messages) == 0
    await mockserver.next_message_future()
    assert len(mockserver.messages) == 1

    # It should return a FindNodeResponse
    response = mockserver.messages[0]
    assert response.nonce == request.nonce
    assert response.HasField('findNodeResponse')

@pytest.mark.asyncio
async def test_responds_to_find_value_when_has_value():
    'When you run a Server and send it STORE / FIND_VALUE it gives you the value'
    mockserver = await startmockserver(3000)

    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    remote_node = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))

    request = messages.Store(key=ID(0b100), value=b'abc').finalize(remote_node)
    mockserver.send(request)

    assert len(mockserver.messages) == 0
    await asyncio.wait_for(mockserver.next_message_future(), timeout=0.1)
    assert len(mockserver.messages) == 1

    request = messages.FindValue(ID(0b100)).finalize(remote_node)
    mockserver.send(request)

    await asyncio.wait_for(mockserver.next_message_future(), timeout=0.1)
    assert len(mockserver.messages) == 2

    # It should return a FoundValue
    response = mockserver.messages[1]
    assert response.nonce == request.nonce
    assert response.HasField('foundValue')
    assert response.foundValue.key == ID(0b100).to_bytes()
    assert response.foundValue.value == b'abc'

@pytest.mark.asyncio
async def test_sending_find_node_response():
    # start our server
    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    remote = core.Node(addr='localhost', port=3001, nodeid=ID(0b1001))
    nodes = [core.Node(addr='localhost', port=i, nodeid=ID(i)) for i in range(5)]

    mockserver = await startmockserver(3000)

    # schedule a response to be sent when our mock receives the FindNode
    future = mockserver.next_message_future()
    def respond(future):
        request = future.result()
        response = messages.FindNodeResponse(request.nonce, nodes).finalize(remote)
        mockserver.send(response)
    future.add_done_callback(respond)

    # send FIND_NODE and see that we correctly parse the response!
    targetnodeid = ID(0b1010)
    result = await asyncio.wait_for(server.find_node(remote, targetnodeid), timeout=0.1)

    assert len(mockserver.messages) == 1  # the mock server received a message
    assert mockserver.messages[0].HasField('findNode')  # Server sent a FindNode message
    assert result.nodes == nodes  # Server parsed the nodes we gave it


@pytest.mark.asyncio
async def test_node_lookup_no_peers():
    'Nothing strange happens when there are no remote nodes'
    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    await server.node_lookup(ID(0b1010))


@pytest.mark.asyncio
async def test_node_lookup_one_empty_peer():
    'We know of one remote peer which knows of nobody'
    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    remote = protocol.Server(mynodeid=ID(0b1001))
    await remote.listen('localhost', 3001)

    server.table.node_seen(remote.node)

    await server.node_lookup(ID(0b1010))


@pytest.mark.asyncio
async def test_node_lookup_finds_peer_through_peers():
    'There exists 2 hops between us and the final peer'
    server = protocol.Server(mynodeid=ID(0b1000))
    await server.listen('localhost', 3000)

    first_hop = protocol.Server(mynodeid=ID(0b1001))
    await first_hop.listen('localhost', 3001)

    second_hop = protocol.Server(mynodeid=ID(0b1010))
    await second_hop.listen('localhost', 3002)

    targetid = ID(0b1011)

    final = protocol.Server(mynodeid=targetid)
    await final.listen('localhost', 3003)

    server.table.node_seen(first_hop.node)
    first_hop.table.node_seen(second_hop.node)
    second_hop.table.node_seen(final.node)

    # If we perform a node lookup we learn of the final node
    with pytest.raises(KeyError):
        server.table.last_seen_for(targetid)
    with pytest.raises(KeyError):
        server.table.last_seen_for(second_hop.node.nodeid)

    await server.node_lookup(targetid)

    assert server.table.last_seen_for(second_hop.node.nodeid) is not None
    assert server.table.last_seen_for(targetid) is not None
