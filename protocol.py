import asyncio
import binascii
import hashlib
import random

import google.protobuf

from protobuf.rpc_pb2 import Message, Ping

def newnonce():
    return hashlib.sha1(random.getrandbits(64)).digest()

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

        if message.HasField('ping'):
            self.ping_received(message, addr)
            return

        print(message)
        print(f'got a real message! {message.nonce}')

        ping = create_ping()
        hexed = binascii.hexlify(ping.SerializeToString())
        print(hexed)
        self.transport.sendto(hexed, addr)

    def ping_received(self, message, addr):
        print(f'received a ping from {message.sender.nodeid}, {addr}')

        ping = create_pong(message)
        data = ping.SerializeToString()
        self.transport.sendto(data, addr)

def create_ping() -> Message:
    message = Message()
    message.sender.ip = 'localhost'
    message.sender.port = 9000
    message.sender.nodeid = 'hi'
    message.sender.publickey = b'hi'

    message.signature = b'xxx'
    message.nonce = 10

    message.ping.SetInParent()

    return message

def create_pong(ping: Message) -> Message:
    message = Message()

    message.sender.ip = 'localhost'
    message.sender.port = 9000
    message.sender.nodeid = 'hi'

    message.nonce = ping.nonce
    message.pong.SetInParent()

    return message

async def main():
    loop = asyncio.get_event_loop()

    transport, protocol = await loop.create_datagram_endpoint(
            lambda: Protocol(),
            local_addr = ('localhost', 3000)
    )

    try:
        await asyncio.sleep(1000)
    finally:
        transport.close()

if __name__ == '__main__':
    asyncio.run(main())
