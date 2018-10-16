import ipaddress
import typing
import asyncio

import core
import protocol

class Node():
    def __init__(self, addr: core.Address, port: int, k: int = 8):
        self.k = k  # the size of each k-bucket, use 1 if nodes will never fail
        self.addr = addr
        self.port = port
        self.nodeid = core.new_node_id()

        self.node = core.Node(addr=addr, port=port, nodeid=self.nodeid)
        self.server = protocol.Server()
        self.table = core.RoutingTable(k, self.nodeid)

    async def listen(self):
        await self.server.listen(self.port)

    async def _refresh(self, bucket: int):
        '''
        Runs when we haven't heard from any of the nodes in this bucket for over an hour
        '''

        # 1. come up with a random ID in the bucket's range
        nodeid = core.random_key_in_bucket(self.nodeid, bucket)

        # 2. perform a FIND_NODE for the ID
        await self.server.find_node(nodeid)

    async def bootstrap(self, address: Address, port:int):
        '''
        Given a node we should connect to, populate our routing table
        '''

        # insert the remote node into the relevant k-bucket
        # this requires first running a PING to get the remote node's ID
        # TODO: handle timeouts!
        pong = await self.server.ping(address, port)

        # perform a FIND_NODE for your own ID
        await self.server.find_node(self.nodeid)

        # refresh all buckets further away than our closest neighbor
        pass

