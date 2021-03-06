from __future__ import annotations

import asyncio
import binascii
import collections
import functools
import hashlib
import heapq
import itertools
import logging
import queue
import random
import typing

import google.protobuf

import core
import messages
from protobuf.rpc_pb2 import Message, Ping, Node as NodeProto


logger = logging.getLogger('kademlia')


class ValueFound(Exception):
    def __init__(self, value: bytes):
        self.value = value


class Protocol(asyncio.DatagramProtocol):

    def __init__(self, table: core.RoutingTable, node: core.Node, rpc_hook):
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()
        self.table = table
        self.node = node

        self.rpc_hook = rpc_hook

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # We're throwing away addr but it seems useful.

        # The node claims to have the address {message.sender}, but {addr} has been proven
        # to work and potentially even punched through a NAT, it seems the better choice!
        try:
            protobuf = Message()
            protobuf.ParseFromString(data)
        except google.protobuf.message.DecodeError:
            logger.warning(f"received malformed data from {addr}")
            return

        message = messages.Message.parse_protobuf(protobuf)

        remote = message.sender
        if remote.nodeid == self.node.nodeid:
            assert False, 'received a message from ourselves'
        try:
            self.table.node_seen(remote)
        except core.NoRoomInBucket:
            # TODO: do something here, we should try to evict a node!
            pass

        if isinstance(message, messages.Response):
            nonce = message.nonce
            if nonce not in self.outstanding_requests:
                logger.warning(f"received malformed data from {addr}")
                return

            future = self.outstanding_requests.pop(nonce)
            future.set_result(message)
            return

        self.rpc_hook(message)

    # Futures

    def register_nonce(self, nonce, future):
        assert(nonce not in self.outstanding_requests)
        self.outstanding_requests[nonce] = future


class Server:
    def __init__(self, mynodeid: core.ID, constants: core.Constants = None):
        self.transport = None
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()

        self.constants = constants if constants is not None else core.Constants()
        self.table = core.RoutingTable(self.constants.k, mynodeid)
        self.storage: typing.Dict[int, bytes] = dict()

        self.node = None
        self.nodeid = mynodeid

    async def listen(self, addr, port):
        loop = asyncio.get_running_loop()
        local_addr = (addr, port)

        self.node = core.Node(addr=addr, port=port, nodeid=self.nodeid)

        endpoint = loop.create_datagram_endpoint(
            lambda: Protocol(self.table, self.node, self.received_rpc),
            local_addr = local_addr
        )
        self.transport, self.protocol = await endpoint

    def must_be_running(func):
        @functools.wraps(func)
        def run(self, *args, **kwargs):
            if not self.transport:
                raise Exception('the server is not running yet!')
            return func(self, *args, **kwargs)
        return run

    def stop(self):
        if self.transport:
            self.transport.close()
            self.transport = None

    @must_be_running
    def send(self, message: messages.Message, remote: core.Node):
        if remote == self.node:
            raise Exception("we've been asked to send a message to ourself!")
        return self.send_to(message, remote.addr, remote.port)

    @must_be_running
    def send_to(self, message: messages.Message, addr, port):
        '''
        Sends the message and returns a future. The Future will be triggered when the
        remote node sends a response to this message.
        '''
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        # when a response comes in with this nonce Protocol will trigger the future
        nonce = message.nonce
        self.protocol.register_nonce(nonce, future)

        # TODO: where do we check that the message is not too large?
        message = message.finalize(self.node)
        serialized = message.SerializeToString()
        self.transport.sendto(serialized, (addr, port))

        # TODO: also timeout if we haven't received a response in x seconds
        # TODO: when a timeout happens, alert the RoutingTable so we mark this node flaky
        return future

    def received_rpc(self, message):
        if isinstance(message, messages.FindNode):
            self.find_node_received(message)
        elif isinstance(message, messages.Ping):
            self.ping_received(message)
        elif isinstance(message, messages.Store):
            self.store_received(message)
        elif isinstance(message, messages.FindValue):
            self.find_value_received(message)
        else:
            assert False, 'an unexpected message type was received'

    # Incoming RPCs

    def _respond(self, request, response: messages.Message):
        finalized = response.finalize(self.node)
        serialized = finalized.SerializeToString()
        dest = (request.sender.addr, request.sender.port)
        self.transport.sendto(serialized, dest)

    def ping_received(self, message):
        logger.debug(f'received a Ping from {message.sender.nodeid}, {message.sender.port}')

        ping = messages.Pong(message.nonce)
        self._respond(message, ping)

    def store_received(self, message):
        logger.debug(f'received a Store from {message.sender.nodeid}, {message.sender.port}')
        self.storage[message.key.value] = message.value

        response = messages.StoreResponse(message.nonce)
        self._respond(message, response)

    def find_node_received(self, request):
        logger.debug(f'received a FindNode from {request.sender.nodeid}, {request.sender.port}')
        # look in the table and return the nodes closest to the requested node
        targetnodeid: core.ID = request.key
        closest: typing.List[core.Node] = self.table.closest(targetnodeid)

        response = messages.FindNodeResponse(request.nonce, closest)
        self._respond(request, response)

    def find_value_received(self, request):
        logger.debug(f'received a FindValue from {request.sender.nodeid}, {request.sender.port}')
        # if we have the value locally reply with a FoundValue
        targetkey: core.ID = request.key
        if targetkey.value in self.storage:
            response = messages.FoundValue(
                request.nonce, targetkey, self.storage[targetkey.value]
            )
            self._respond(request, response)
            return

        # otherwise, return the nodes most likely to have the value
        self.find_node_received(request)

    # Outbound RPCs

    @must_be_running
    async def ping(self, addr, port: int, timeout: int = None):
        pingmsg = messages.Ping()
        future = self.send_to(pingmsg, addr, port)
        result = None
        if timeout is None:
            result = await future
        else:
            result = await asyncio.wait_for(future, timeout=timeout)
        assert isinstance(result, messages.Pong)

    @must_be_running
    async def find_node(self, remote: core.Node, targetnodeid: core.ID) -> typing.List[core.Node]:
        'Send a FIND_NODE to remote and return the result'
        message = messages.FindNode(targetnodeid)
        future = self.send(message, remote)
        result = await future
        # TODO: throw an error if we weren't given a FindNodeResponse
        return result

    @must_be_running
    async def find_value(self, remote: core.Node, targetnodeid: core.ID) -> typing.List[core.Node]:
        'Send a FIND_VALUE to remote and return the result'
        message = messages.FindValue(targetnodeid)
        future = self.send(message, remote)
        result = await future
        if isinstance(result, messages.FoundValue):
            raise ValueFound(result.value)
        return result

    @must_be_running
    async def store(self, remote: core.Node, key: core.ID, value: bytes):
        # todo: write a test for this function
        message = messages.Store(key, value)
        future = self.send(message, remote)
        result = await future
        return  # TODO: look at and verify the result

    # Node lookups

    @must_be_running
    async def node_lookup(self, targetnodeid: core.ID) -> typing.List[core.Node]:
        return await self._lookup(targetnodeid, looking_for_value=False)

    @must_be_running
    async def value_lookup(self, targetnodeid: core.ID):
        try:
            await self._lookup(targetnodeid, looking_for_value=True)
        except ValueFound as ex:
            return ex.value

    @must_be_running
    async def _lookup(self, targetnodeid: core.ID, looking_for_value: bool) -> typing.List[core.Node]:
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
        # start with the alpha nodes closest to me
        to_query = self.table.closest_to_me(self.constants.alpha)

        queried = collections.defaultdict(lambda: False)
        seen_nodes = list()

        while True:
            rpc_coro = self.find_value if looking_for_value else self.find_node
            coros = [rpc_coro(node, targetnodeid) for node in to_query]

            for node in to_query:
                queried[node.nodeid] = True

            # collect all the responses, merge them into our list, keep the closest k
            results = await asyncio.gather(*coros)  # TODO: set some kind of timeout!
            new_nodes = (node for result in results for node in result.nodes)
            new_nodes = (node for node in new_nodes if node.nodeid != self.nodeid)
            seen_nodes = sorted(
                itertools.chain(seen_nodes, new_nodes),
                key=lambda node: node.nodeid.distance(targetnodeid)
            )[:self.constants.k]

            # for the next round, send queries to alpha of the closest unqueried nodes
            to_query = list(itertools.islice(
                (node for node in seen_nodes if node.nodeid not in queried),
                self.constants.alpha
            ))

            # finish once you've queried all of the k closest nodes you know of
            if len(to_query) == 0:
                break

        return seen_nodes
