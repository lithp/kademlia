import ipaddress
import typing
import asyncio

import core

class Node():
    def __init__(self, addr: core.Address, port: int, k: int = 8):
        self.k = k  # the size of each k-bucket, use 1 if nodes will never fail
        self.addr = addr
        self.port = port
        self.nodeid = core.new_node_id()
        self.node = core.Node(addr=addr, port=port, nodeid=self.nodeid)

        # start listening on a port

    def listen():
        pass

    def _refresh(bucket: int):
        '''
        Runs when we haven't heard from any of the nodes in this bucket for over an hour
        '''

        # 1. come up with a random ID in the bucket's range
        # 2. perform a FIND_NODE for the ID
        pass

    def bootstrap(address: Address, port:int):
        '''
        Given a node we should connect to, populate our routing table
        '''

        # insert the remote node into the relevant k-bucket

        # perform a FIND_NODE for your own ID

        # refresh all buckets further away than our closest neighbor
        pass

