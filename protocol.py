import asyncio
import binascii
import hashlib
import random

import google.protobuf

import core
from protobuf.rpc_pb2 import Message, Ping

def newnonce():
    return random.getrandbits(160).to_bytes(20, byteorder='big')

class Protocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        try:
            message = Message()
            message.ParseFromString(data)
        except google.protobuf.message.DecodeError:
            print(f"received malformed data from {addr}")
            print(data)
            return

        # We're throwing away addr but it seems useful.

        # The node claims to have the address {message.sender}, but {addr} has been proven
        # to work and potentially even punched through a NAT, it seems the better choice!

        if message.HasField('ping'):
            self.ping_received(message)
            return

        print(message)
        print(f'got a real message! {message.nonce}')

        ping = create_ping()
        hexed = binascii.hexlify(ping.SerializeToString())
        print(hexed)
        self.transport.sendto(hexed, addr)

    # Requests

    def ping_received(self, message):
        '''
        Tell our node that a peer was seen!
        '''
        print(f'received a ping from {message.sender.nodeid}, {message.sender.port}')

        ping = create_pong(message)
        data = ping.SerializeToString()

        addr = (message.sender.ip, message.sender.port)
        self.transport.sendto(data, addr)

    def store_received(self, message):
        pass

    def find_node_received(self, message):
        pass

    def find_value_received(self, message):
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

# This should use data from our Node!
def create_ping() -> Message:
    message = Message()
    message.sender.ip = 'localhost'
    message.sender.port = 9000
    message.sender.nodeid = newnonce()
    message.sender.publickey = b'hi'

    message.signature = b'xxx'
    message.nonce = newnonce()

    message.ping.SetInParent()

    return message

def create_pong(ping: Message) -> Message:
    message = Message()

    message.sender.ip = 'localhost'
    message.sender.port = 9000
    message.sender.nodeid = newnonce()

    message.nonce = ping.nonce
    message.pong.SetInParent()

    return message

class Server:

    def __init__(self):
        self.transport = None

    async def listen(self, port):
        loop = asyncio.get_event_loop()
        local_addr = ('localhost', port)

        endpoint = loop.create_datagram_endpoint(
            lambda: Protocol(), local_addr = local_addr
        )
        self.transport, self.protocol = await endpoint

    def stop(self):
        if self.transport:
            self.transport.close()

async def main():

    server = Server()
    await server.listen(3000)

    try:
        await asyncio.sleep(1000)
    finally:
        server.stop()

if __name__ == '__main__':
    asyncio.run(main())
