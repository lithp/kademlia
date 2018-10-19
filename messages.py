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
    def nonce():
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
