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

        print(message)
        print(f'got a real message! {message.nonce}')

        ping = create_ping()
        hexed = binascii.hexlify(ping.SerializeToString())
        print(hexed)
        self.transport.sendto(hexed, addr)

        # you can call self.transport.sendto(data, addr) if you want to send data

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
