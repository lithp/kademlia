from __future__ import annotations

import asyncio
import binascii
import collections
import hashlib
import heapq
import itertools
import queue
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

def read_nodeid(asbytes: bytes) -> core.ID:
    return core.ID.from_bytes(asbytes)

def write_nodeid(nodeid: core.ID) -> bytes:
    return nodeid.to_bytes()

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

class MessageBuilder:
    def __init__(self, node: core.Node):
        self.node = node
    def find_node_response(self, nonce: bytes, nodes: typing.List[core.Node]):
        message = create_response(self.node, nonce)
        for node in nodes:
            neighbor = message.findNodeResponse.neighbors.add()
            neighbor.ip = node.addr
            neighbor.port = node.port
            neighbor.nodeid = write_nodeid(node.nodeid)
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

def create_find_node(node: core.Node, targetnodeid: core.ID) -> Message:
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

def create_store(node: core.Node, key: core.ID, value: bytes) -> Message:
    message = create_message(node)
    message.store.key = write_nodeid(key)
    message.store.value = value
    return message

def create_found_value(node: core.Node, nonce: bytes, key: core.ID, value: bytes) -> Message:
    message = create_response(node, nonce)
    message.foundValue.key = write_nodeid(key)
    message.foundValue.value = value
    return message

class ValueFound(Exception):
    def __init__(self, value: bytes):
        self.value = value

def create_find_value(node: core.Node, key: core.ID) -> Message:
    message = create_message(node)
    message.findValue.key = write_nodeid(key)
    return message

def parse_find_node_response(response: Message) -> typing.List[core.Node]:
    return [
        core.Node(
            addr=neighbor.ip,
            port=neighbor.port,
            nodeid=read_nodeid(neighbor.nodeid)
        ) for neighbor in response.findNodeResponse.neighbors
    ]

class Protocol(asyncio.DatagramProtocol):

    def __init__(self, table: core.RoutingTable, node: core.Node):
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()
        self.table = table
        self.node = node

        # TODO: figure out a better place to put this
        self.storage: typing.Dict[int, bytes] = dict()
        self.build = MessageBuilder(self.node)

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
        if remote.nodeid == self.node.nodeid:
            assert False, 'received a message from ourselves'
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
        self.storage[read_nodeid(message.store.key).value] = message.store.value

        response = create_store_response(self.node, message.nonce)
        self._respond(message, response)

    def find_node_received(self, request):
        # look in the table and return the nodes closest to the requested node
        targetnodeid: core.ID = read_nodeid(request.findNode.key)
        closest: typing.List[core.Node] = self.table.closest(targetnodeid)

        response = self.build.find_node_response(request.nonce, closest)
        self._respond(request, response)

    def find_value_received(self, request):
        # if we have the value locally reply with a FoundValue
        targetkey: core.ID = read_nodeid(request.findValue.key)
        if targetkey.value in self.storage:
            response = create_found_value(
                self.node, request.nonce, targetkey, self.storage[targetkey.value]
            )
            self._respond(request, response)
            return

        # otherwise, return the nodes most likely to have the value
        self.find_node_received(request)


class Server:
    def __init__(self, k: int, mynodeid: core.ID):
        self.transport = None
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()

        self.table = core.RoutingTable(k, mynodeid)

        self.k = k
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

    def send(self, message, remote: core.Node):
        '''
        Sends the message and returns a future. The Future will be triggered when the
        remote node sends a response to this message.
        '''
        if not self.transport:
            raise Exception('the server is not running yet!')

        if remote == self.node:
            raise Exception("we've been asked to send a message to ourself!")

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        # when a response comes in with this nonce Protocol will trigger the future
        nonce = message.nonce
        self.protocol.register_nonce(nonce, future)

        addr = (remote.addr, remote.port)

        # TODO: where do we check that the message is not too large?
        serialized = message.SerializeToString()
        self.transport.sendto(serialized, addr)

        # TODO: also timeout if we haven't received a response in x seconds
        # TODO: when a timeout happens, alert the RoutingTable so we mark this node flaky
        return future

    async def ping(self, remote):
        if not self.transport:
            raise Exception('the server is not running yet!')
        pingmsg = create_ping(self.node)
        future = self.send(pingmsg, remote)
        # TODO: timeout if this takes too long?
        # TODO: check that we were given back a pong?
        await future

    async def node_lookup(self, targetnodeid: core.ID) -> typing.List[core.Node]:
        return await self._lookup(targetnodeid, looking_for_value=False)

    async def value_lookup(self, targetnodeid: core.ID):
        try:
            await self._lookup(targetnodeid, looking_for_value=True)
        except ValueFound as ex:
            return ex.value

    async def _lookup(self, targetnodeid: core.ID, looking_for_value: bool) -> typing.List[core.Node]:
        alpha = 3
        if not self.transport:
            raise Exception('the server is not running yet!')
        # start with the alpha nodes closest to me
        to_query = self.table.closest_to_me(alpha)

        queried = collections.defaultdict(lambda: False)
        seen_nodes = list()

        while True:
            rpc_coro = self.find_value if looking_for_value else self.find_node
            coros = [rpc_coro(node, targetnodeid) for node in to_query]

            for node in to_query:
                queried[node.nodeid] = True

            # collect all the responses, merge them into our list, keep the closest k
            results = await asyncio.gather(*coros)  # TODO: set some kind of timeout!
            if looking_for_value:
                pass
            new_nodes = (node for result in results for node in result)
            new_nodes = (node for node in new_nodes if node.nodeid != self.nodeid)
            seen_nodes = sorted(
                itertools.chain(seen_nodes, new_nodes),
                key=lambda node: node.nodeid.distance(targetnodeid)
            )[:self.k]

            # for the next round, send queries to alpha of the closest unqueried nodes
            to_query = list(itertools.islice(
                (node for node in seen_nodes if node.nodeid not in queried),
                alpha
            ))

            # finish once you've queried all of the k closest nodes you know of
            if len(to_query) == 0:
                break

        return seen_nodes
        '''
        A way you might be able to parallalize this:
        1. always have alpha requests in-flight
        2. keep track of the k closest nodes to your target
           (add to this list as responses come in)
        3. don't query any node more than once
        4. quit when you've queried all of the k-closest nodes you know of
        - this isn't quite right:
          you want to hold onto more than k nodes, nodes which never respond are removed
          from your list (until they do respond) and you continue until you've heard back
          from the k-closest nodes still in consideration
        '''

    async def find_node(self, remote: core.Node, targetnodeid: core.ID) -> typing.List[core.Node]:
        'Send a FIND_NODE to remote and return the result'
        if not self.transport:
            raise Exception('the server is not running yet!')
        message = create_find_node(self.node, targetnodeid)
        future = self.send(message, remote)
        result = await future
        # TODO: throw an error if we weren't given a FindNodeResponse
        return parse_find_node_response(result)

    async def find_value(self, remote: core.Node, targetnodeid: core.ID) -> typing.List[core.Node]:
        'Send a FIND_VALUE to remote and return the result'
        if not self.transport:
            raise Exception('the server is not running yet!')
        message = create_find_value(self.node, targetnodeid)
        future = self.send(message, remote)
        result = await future
        if result.HasField('foundValue'):
            raise ValueFound(result.foundValue.value)
        return parse_find_node_response(result)

    async def store(self, remote: core.Node, key: core.ID, value: bytes):
        if not self.transport:
            raise Exception('the server is not running yet!')
        # todo: write a test for this function
        message = create_store(self.node, key, value)
        future = self.send(message, remote)
        result = await future
        return  # TODO: look at and verify the result

