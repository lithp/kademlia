import asyncio
import binascii
import hashlib
import random

import google.protobuf

import core
from protobuf.rpc_pb2 import Message, Ping

def newnonce():
    return random.getrandbits(160).to_bytes(20, byteorder='big')

def isresponse(message):
    responses = ['pong', 'storeResponse', 'findNodeResponse', 'foundValue']
    return any(message.HasField(response) for response in responses)

class Protocol(asyncio.DatagramProtocol):

    def __init__(self):
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()

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

        # TODO: tell the routing table about message.sender

        if isresponse(message):
            nonce = message.nonce
            if nonce not in self.outstanding_requests:
                print(f"received a message but the nonce is not recognized")
                return

            future = self.outstanding_requests.pop(nonce)
            future.set_result(message)
            return

        # If it's a request, call the appropriate handler!

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

    # Futures

    def register_nonce(self, nonce, future):
        assert(nonce not in self.outstanding_requests)
        self.outstanding_requests[nonce] = future

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
        # if we have the value locally reply with a FoundValue
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
        self.outstanding_requests: typing.Dict[bytes, asyncio.Future] = dict()

    async def listen(self, port):
        loop = asyncio.get_running_loop()
        local_addr = ('localhost', port)

        endpoint = loop.create_datagram_endpoint(
            lambda: Protocol(), local_addr = local_addr
        )
        self.transport, self.protocol = await endpoint

    def stop(self):
        if self.transport:
            self.transport.close()
            self.transport = None

    async def send(self, message, addr):
        '''
        1. Create a future and associate it with the nonce, so that when we get a response
           we 
        2. return the future
        3. Later, the Protocol will alert us that a message has arrived
           (and we'll set the Future)
        '''
        if not self.transport:
            raise Exception('the server is not running yet!')

        loop = asyncio.get_running_loop()
        future = loop.create_future()

        # when a response come in with this nonce the protocol will trigger the future
        nonce = message.nonce
        self.protocol.register_nonce(nonce, future)

        # TODO: where do we check that the message is not too large?
        serialized = message.SerializeToString()
        await self.transport.sendto(serialized, addr)

        # TODO: also timeout if we haven't received a response in x seconds
        # TODO: when a timeout happens, alert the RoutingTable so we mark this node flaky
        return future

async def main():

    server = Server()
    await server.listen(3000)

    try:
        await asyncio.sleep(1000)
    finally:
        server.stop()

if __name__ == '__main__':
    asyncio.run(main())
