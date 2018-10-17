import asyncio
import binascii
import hashlib
import random
import typing

import google.protobuf

import core
from protobuf.rpc_pb2 import Message, Ping, Node as NodeProto

def newnonce():
    return random.getrandbits(160).to_bytes(20, byteorder='big')

def isresponse(message):
    responses = ['pong', 'storeResponse', 'findNodeResponse', 'foundValue']
    return any(message.HasField(response) for response in responses)

def read_nodeid(asbytes: bytes) -> int:
    asint = int.from_bytes(asbytes, byteorder='big')
    assert(asint.bit_length() <= 160)
    return asint

def write_nodeid(asint: int) -> bytes:
    return asint.to_bytes(20, byteorder='big')

def read_node(node: NodeProto) -> core.Node:
    return core.Node(
        addr=node.ip,
        port=node.port,
        nodeid=read_nodeid(node.nodeid)
    )

def create_message(node: core.Node) -> Message:
    message = Message()

    message.sender.ip = node.addr
    message.sender.port = node.port
    message.sender.nodeid = write_nodeid(node.nodeid)

    message.nonce = newnonce()

    return message

def create_response(node: core.Node, request_nonce: bytes) -> Message:
    message = create_message(node)
    message.nonce = request_nonce
    return message

def create_find_node_response(
        node: core.Node, nonce: bytes, nodes: typing.List[core.Node]) -> Message:
    message = create_response(node, nonce)
    for node in nodes:
        neighbor = message.findNodeResponse.neighbors.add()
        neighbor.ip = node.addr
        neighbor.port = node.port
        neighbor.nodeid = write_nodeid(node.nodeid)
    return message

def create_find_node(node: core.Node, targetnodeid: int) -> Message:
    message = create_message(node)
    message.findNode.key = write_nodeid(targetnodeid)
    return message

def create_ping(node: core.Node) -> Message:
    message = create_message(node)
    message.ping.SetInParent()
    return message

def create_pong(node: core.Node, nonce: bytes) -> Message:
    message = create_response(node, nonce)
    message.pong.SetInParent()
    return message

def create_store_response(node: core.Node, nonce: bytes) -> Message:
    message = create_response(node, nonce)
    message.storeResponse.SetInParent()
    return message

def create_store(node: core.Node, key: bytes, value: bytes) -> Message:
    message = create_message(node)
    message.store.key = key
    message.store.value = value
    return message

def create_found_value(node: core.Node, nonce: bytes, key: bytes, value: bytes) -> Message:
    message = create_response(node, nonce)
    message.foundValue.key = key
    message.foundValue.value = value
    return message

def create_find_value(node: core.Node, key: bytes) -> Message:
    message = create_message(node)
    message.findValue.key = key
    return message

class Protocol(asyncio.DatagramProtocol):

    def __init__(self, table: core.RoutingTable, node: core.Node):
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()
        self.table = table
        self.node = node

        # TODO: figure out a better place to put this
        self.storage: typing.Dict[bytes, bytes] = dict()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # We're throwing away addr but it seems useful.

        # The node claims to have the address {message.sender}, but {addr} has been proven
        # to work and potentially even punched through a NAT, it seems the better choice!
        try:
            message = Message()
            message.ParseFromString(data)
        except google.protobuf.message.DecodeError:
            print(f"received malformed data from {addr}")
            print(data)
            return

        remote = read_node(message.sender)
        try:
            self.table.node_seen(remote)
        except core.NoRoomInBucket:
            # TODO: do something here, we should try to evict a node!
            pass

        if isresponse(message):
            nonce = message.nonce
            if nonce not in self.outstanding_requests:
                print(f"received a message but the nonce is not recognized")
                return

            future = self.outstanding_requests.pop(nonce)
            future.set_result(message)
            return

        if message.HasField('findNode'):
            self.find_node_received(message)
            return

        if message.HasField('ping'):
            self.ping_received(message)
            return

        if message.HasField('store'):
            self.store_received(message)
            return

        if message.HasField('findValue'):
            self.find_value_received(message)
            return

        assert False, 'an unexpected message type was received'

    # Futures

    def register_nonce(self, nonce, future):
        assert(nonce not in self.outstanding_requests)
        self.outstanding_requests[nonce] = future

    # Requests

    def _respond(self, request, response):
        serialized = response.SerializeToString()
        dest = (request.sender.ip, request.sender.port)
        self.transport.sendto(serialized, dest)

    def ping_received(self, message):
        # TODO: turn this into a logging statement
        print(f'received a ping from {message.sender.nodeid}, {message.sender.port}')

        ping = create_pong(self.node, message.nonce)
        self._respond(message, ping)

    def store_received(self, message):
        self.storage[message.store.key] = message.store.value

        response = create_store_response(self.node, message.nonce)
        self._respond(message, response)

    def find_node_received(self, request):
        # look in the table and return the nodes closest to the requested node
        targetnodeid: int = read_nodeid(request.findNode.key)
        closest: typing.List[core.Node] = self.table.closest(targetnodeid)

        response = create_find_node_response(self.node, request.nonce, closest)
        self._respond(request, response)

    def find_value_received(self, request):
        # if we have the value locally reply with a FoundValue
        targetkey = request.findValue.key
        if targetkey in self.storage:
            response = create_found_value(
                self.node, request.nonce, targetkey, self.storage[targetkey]
            )
            self._respond(request, response)
            return

        # otherwise, return the nodes most likely to have the value
        self.find_node_received(request)


class Server:

    def __init__(self, k: int, mynodeid: int):
        self.transport = None
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()

        self.table = core.RoutingTable(k, mynodeid)

        self.node = None
        self.nodeid = mynodeid

    async def listen(self, port):
        loop = asyncio.get_running_loop()
        local_addr = ('localhost', port)

        self.node = core.Node(addr='localhost', port=port, nodeid=self.nodeid)

        endpoint = loop.create_datagram_endpoint(
            lambda: Protocol(self.table, self.node), local_addr = local_addr
        )
        self.transport, self.protocol = await endpoint

    def stop(self):
        if self.transport:
            self.transport.close()
            self.transport = None

    def send(self, message, addr):
        '''
        Sends the message and returns a future. The Future will be triggered when the
        remote node sends a response to this message.
        '''
        if not self.transport:
            raise Exception('the server is not running yet!')

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        # when a response comes in with this nonce Protocol will trigger the future
        nonce = message.nonce
        self.protocol.register_nonce(nonce, future)

        # TODO: where do we check that the message is not too large?
        serialized = message.SerializeToString()
        self.transport.sendto(serialized, addr)

        # TODO: also timeout if we haven't received a response in x seconds
        # TODO: when a timeout happens, alert the RoutingTable so we mark this node flaky
        return future

    async def ping(self, addr):
        pingmsg = create_ping(self.node)
        future = self.send(pingmsg, addr)
        await future

    async def find_node(self, nodeid: int):

        # you'll hopefully receive a FindNodeResponse, which contains a list of nodes
        # which are closest to the requested id

        pass
