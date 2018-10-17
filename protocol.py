import asyncio
import binascii
import hashlib
import random
import typing

import google.protobuf

import core
from protobuf.rpc_pb2 import Message, Ping

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

class Protocol(asyncio.DatagramProtocol):

    def __init__(self, table: core.RoutingTable, node: core.Node):
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()
        self.table = table
        self.node = node

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

        # TODO: Parsing the message belongs elsewhere
        remote = core.Node(
            addr=message.sender.ip,
            port=message.sender.port,
            nodeid=read_nodeid(message.sender.nodeid)
        )
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

        # TODO: forward RPCs to store_received and find_value_received


    # Futures

    def register_nonce(self, nonce, future):
        assert(nonce not in self.outstanding_requests)
        self.outstanding_requests[nonce] = future

    # Requests

    def ping_received(self, message):
        print(f'received a ping from {message.sender.nodeid}, {message.sender.port}')

        ping = create_pong(self.node, message.nonce)
        data = ping.SerializeToString()

        addr = (message.sender.ip, message.sender.port)
        self.transport.sendto(data, addr)

    def store_received(self, message):
        pass

    def find_node_received(self, message):
        # look in the table and return the nodes closest to the requested node
        targetnodeid: int = read_nodeid(message.findNode.key)
        closest: typing.List[core.Node] = self.table.closest(targetnodeid)

        message = create_message(self.node)
        create_find_node_response(message, closest)
        serialized = message.SerializeToString()

        addr = (message.sender.ip, message.sender.port)
        self.transport.sendto(serialized, addr)

    def find_value_received(self, message):
        # if we have the value locally reply with a FoundValue
        pass

    # Responses

    def pong_received(self, message):
        # check the nonce!
        # now forward this to the coroutine which was waiting for it
        #  (lookup the future by nonce)
        pass

    def find_node_response_received(self, message):
        pass

    def found_value_received(self, message):
        pass

def create_message(node: core.Node) -> Message:
    message = Message()

    message.sender.ip = node.addr
    message.sender.port = node.port
    message.sender.nodeid = write_nodeid(node.nodeid)

    message.nonce = newnonce()

    return message

def create_find_node_response(stub: Message, nodes: typing.List[core.Node]):
    for node in nodes:
        neighbor = stub.neighbors.add()
        neighbor.ip = node.addr
        neighbor.port = node.port
        neighbor.nodeid = write_nodeid(node.nodeid)

# This should use data from our Node!
def create_ping(node: core.Node) -> Message:
    message = Message()

    message.sender.ip = node.addr
    message.sender.port = node.port
    message.sender.nodeid = write_nodeid(node.nodeid)

    message.nonce = newnonce()

    message.ping.SetInParent()

    return message

def create_pong(node: core.Node, nonce: bytes) -> Message:
    message = Message()

    message.sender.ip = node.addr
    message.sender.port = node.port
    message.sender.nodeid = write_nodeid(node.nodeid)

    message.nonce = nonce
    message.pong.SetInParent()

    return message

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
