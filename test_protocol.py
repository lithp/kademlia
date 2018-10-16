from protocol import newnonce

def test_newnonce():
    nonce = newnonce()
    assert len(nonce) == 20
