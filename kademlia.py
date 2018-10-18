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
        self.server = protocol.Server(k, self.nodeid)

    async def listen(self):
        await self.server.listen(self.port)

    async def _refresh(self, bucket: int):
        '''
        Runs when we haven't heard from any of the nodes in this bucket for over an hour
        '''
        nodeid = core.random_key_in_bucket(self.nodeid, bucket)
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
        remote = core.Node(addr=address, port=port, nodeid=b'garbage')
        await self.server.ping(remote)

        # perform a node lookup for your own ID
        await self.server.node_lookup(self.nodeid)

        # refresh all buckets further away than our closest neighbor

        # TODO: it's a smell that we have to dig so deeply to find the table
        # TODO: it's probably safe to do these in parallel?
        closest = self.server.table.first_occupied_bucket()
        for index in range(closest, 160):
            await self._refresh(index)

