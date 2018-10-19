import google.protobuf
import typing

import core
import protocol
import protobuf.rpc_pb2 as proto


class Message:
    def __init__(self):
        self.message = proto.Message()
        self.message.nonce = protocol.newnonce()

    @property
    def nonce(self):
        return self.message.nonce

    def finalize(self, node: core.Node):
        message = self.message
        message.sender.ip = node.addr
        message.sender.port = node.port
        message.sender.nodeid = node.nodeid.to_bytes()
        return message

class Response(Message):
    def __init__(self, nonce: bytes):
        super().__init__()
        self.message.nonce = nonce

class FindNodeResponse(Response):
    def __init__(self, nonce, nodes: typing.List[core.Node]):
        super().__init__(nonce)
        for node in nodes:
            neighbor = self.message.findNodeResponse.neighbors.add()
            neighbor.ip = node.addr
            neighbor.port = node.port
            neighbor.nodeid = node.nodeid.to_bytes()

# if there was ever a time for metaclasses

class FindNode(Message):
    def __init__(self, targetnodeid: core.ID):
        super().__init__()
        self.message.findNode.key = targetnodeid.to_bytes()

class Ping(Message):
    def __init__(self):
        super().__init__()
        self.message.ping.SetInParent()

class Pong(Response):
    def __init__(self, nonce):
        super().__init__(nonce)
        self.message.pong.SetInParent()

class StoreResponse(Response):
    def __init__(self, nonce):
        super().__init__(nonce)
        self.message.storeResponse.SetInParent()

class Store(Message):
    def __init__(self, key: core.ID, value: bytes):
        super().__init__()
        self.message.store.key = key.to_bytes()
        self.message.store.value = value

class FoundValue(Response):
    def __init__(self, nonce, key: core.ID, value: bytes):
        super().__init__(nonce)
        self.message.foundValue.key = key.to_bytes()
        self.message.foundValue.value = value

class FindValue(Message):
    def __init__(self, key: core.ID):
        super().__init__()
        self.message.findValue.key = key.to_bytes()
