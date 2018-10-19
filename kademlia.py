import asyncio
import logging
import typing

import core
import protocol


logger = logging.getLogger('kademlia')


class Node():
    def __init__(self, addr: str, port: int, constants: core.Constants = None):
        self.constants = constants if constants is not None else core.Constants()
        self.addr = addr
        self.port = port
        self.nodeid = core.ID()

        self.node = core.Node(addr=addr, port=port, nodeid=self.nodeid)
        self.server = protocol.Server(self.nodeid, self.constants)

    async def listen(self):
        await self.server.listen(self.addr, self.port)

    async def _refresh(self, bucket: int):
        '''
        Should run when we haven't heard from any of the nodes in this bucket for over an
        hour.
        '''
        nodeid: core.ID = core.random_key_in_bucket(self.nodeid, bucket)
        await self.server.node_lookup(nodeid)

    async def bootstrap(self, address: str, port:int):
        '''
        Given a node we should connect to, populate our routing table
        '''

        # 1. Add the remote node to our buckeet
        try:
            await self.server.ping(address, port, timeout=10)
        except asyncio.TimeoutError:
            logger.error('cannot bootstrap, the remote node did not respond')
            return

        # 2. perform a node lookup for your own ID
        await self.server.node_lookup(self.nodeid)

        # 3. refresh all buckets further away than our closest neighbor

        # todo: 160*k requests is probably too many to send at once but this could
        #       definitely be parallelized at least a little
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
        '''
        Perform a node lookup but send FIND_VALUE messages, and stop once we've found the
        value.
        '''
        return await self.server.value_lookup(key)
