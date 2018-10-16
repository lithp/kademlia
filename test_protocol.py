import asyncio
import pytest

import protocol

def test_newnonce():
    nonce = protocol.newnonce()
    assert len(nonce) == 20

@pytest.mark.asyncio  # run the test inside an event loop so we don't have to make one
async def test_nonce_matching():
    'When you send a PING and get back a PONG with the same nonce the future is triggered'

    server = protocol.Server()
    await server.listen(3000)

    ping_message = protocol.create_ping()
    future = server.send(ping_message, ('localhost', 3001))

    # this is a lot of code just to send a UDP packet
    loop = asyncio.get_running_loop()
    transport, dgprotocol = await loop.create_datagram_endpoint(
        asyncio.DatagramProtocol,
        local_addr = ('localhost', 3001)
    )

    await asyncio.sleep(0.1)
    assert not future.done()  # we have not yet sent the message

    pong_message = protocol.create_pong(ping_message)
    pong_message.nonce = b'garbage'
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    await asyncio.sleep(0.1)
    assert not future.done()  # we sent a message with the wrong nonce

    pong_message = protocol.create_pong(ping_message)
    serialized = pong_message.SerializeToString()
    transport.sendto(serialized, ('localhost', 3000))

    # we sent a conforming PONG, the future should now be set!
    try:
        await asyncio.wait_for(future, timeout=0.5)
    except asyncio.TimeoutError:
        assert False, 'the future was never set'

    assert future.done()
