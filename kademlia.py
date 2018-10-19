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
        self.nodeid = core.ID()

        self.node = core.Node(addr=addr, port=port, nodeid=self.nodeid)
        self.server = protocol.Server(k, self.nodeid)

    async def listen(self):
        await self.server.listen(self.port)

    async def _refresh(self, bucket: int):
        '''
        Runs when we haven't heard from any of the nodes in this bucket for over an hour
        '''
        nodeid: core.ID = core.random_key_in_bucket(self.nodeid, bucket)
        await self.server.node_lookup(nodeid)

    async def bootstrap(self, address: core.Address, port:int):
        '''
        Given a node we should connect to, populate our routing table
        '''

        # When the remote node responds it will be added to the relevant k-bucket
        # TODO: handle timeouts!
        # TODO: is this address type correct?
        #       No, it certainly is not
        # TODO: that we have to build a fake node here is likely a bad smell
        remote = core.Node(addr=address, port=port, nodeid=core.ID(0b11111111))
        await self.server.ping(remote)

        # perform a node lookup for your own ID
        await self.server.node_lookup(self.nodeid)

        # refresh all buckets further away than our closest neighbor

        # TODO: it's a smell that we have to dig so deeply to find the table
        # TODO: it's probably safe to do these in parallel?
        closest = self.server.table.first_occupied_bucket()
        for index in range(closest, 160):
            await self._refresh(index)

    async def store_value(self, key: core.ID, value: bytes):
        'Find the k closest nodes and send a STORE RPC to all of them'
        closest_nodes = await self.server.node_lookup(key)

        # todo: timeouts
        coros = [
            self.server.store(node, key, value)
            for node in closest_nodes
        ]
        await asyncio.gather(*coros)

    async def find_value(self, key: core.ID):
        "Perform a node lookup but send FIND_VALUE messages, and stop once it's found"
        return await self.server.value_lookup(key)
