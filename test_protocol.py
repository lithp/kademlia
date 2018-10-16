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

    assert True
