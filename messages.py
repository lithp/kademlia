import dataclasses
import google.protobuf
import typing

import core
import protobuf.rpc_pb2 as proto


@dataclasses.dataclass
class Message:
    message_types: typing.ClassVar = dict()
    nonce: bytes = dataclasses.field(init=False, default_factory=core.newnonce)
    sender: core.Node = dataclasses.field(init=False)

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, 'field'):
            Message.message_types[cls.field] = cls
        super().__init_subclass__(**kwargs)

    def finalize(self, node: core.Node):
        message = proto.Message()
        message.nonce = self.nonce
        message.sender.ip = node.addr
        message.sender.port = node.port
        message.sender.nodeid = node.nodeid.to_bytes()

        if hasattr(self, '_to_proto'):
            self._to_proto(message)

        return message

    @staticmethod
    def _parse_node(sender: proto.Node) -> core.Node:
        return core.Node(
            addr=sender.ip,
            port=sender.port,
            nodeid=core.ID.from_bytes(sender.nodeid)
        )

    @classmethod
    def parse_protobuf(cls, protobuf: proto.Message):
        for fieldName, fieldClass in cls.message_types.items():
            if protobuf.HasField(fieldName):
                result = fieldClass._from_proto(protobuf)
                result.nonce = protobuf.nonce
                result.sender = cls._parse_node(protobuf.sender)
                return result
        raise ValueError(f'did not recognize {proto}')

# if there were any more of these it would barely be worth metaprogramming

@dataclasses.dataclass
class Response(Message):
    nonce: bytes

@dataclasses.dataclass
class FindNode(Message):
    field = 'findNode'
    key: core.ID

    def _to_proto(self, stub):
        stub.findNode.key = self.key.to_bytes()

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls(key=core.ID.from_bytes(proto.findNode.key))


@dataclasses.dataclass
class FindNodeResponse(Response):
    field = 'findNodeResponse'
    nodes: typing.List[core.Node]

    def _to_proto(self, stub):
        for node in self.nodes:
            neighbor = stub.findNodeResponse.neighbors.add()
            neighbor.ip = node.addr
            neighbor.port = node.port
            neighbor.nodeid = node.nodeid.to_bytes()

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls(proto.nonce, [
            core.Node(
                addr=neighbor.ip,
                port=neighbor.port,
                nodeid=core.ID.from_bytes(neighbor.nodeid)
            ) for neighbor in proto.findNodeResponse.neighbors
        ])

class Ping(Message):
    field = 'ping'

    def _to_proto(self, stub):
        stub.ping.SetInParent()

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls()

class Pong(Response):
    field = 'pong'

    def _to_proto(self, stub):
        stub.pong.SetInParent()

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls(proto.nonce)

class StoreResponse(Response):
    field = 'storeResponse'

    def _to_proto(self, stub):
        stub.storeResponse.SetInParent()

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls(proto.nonce)

@dataclasses.dataclass
class Store(Message):
    field = 'store'
    key: core.ID
    value: bytes

    def _to_proto(self, stub):
        stub.store.key = self.key.to_bytes()
        stub.store.value = self.value

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls(core.ID.from_bytes(proto.store.key), proto.store.value)

@dataclasses.dataclass
class FoundValue(Response):
    field = 'foundValue'
    key: core.ID
    value: bytes

    def _to_proto(self, stub):
        stub.foundValue.key = self.key.to_bytes()
        stub.foundValue.value = self.value

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls(
            proto.nonce,
            core.ID.from_bytes(proto.foundValue.key),
            proto.foundValue.value
        )

@dataclasses.dataclass
class FindValue(Message):
    field = 'findValue'
    key: core.ID

    def _to_proto(self, stub):
        stub.findValue.key = self.key.to_bytes()

    @classmethod
    def _from_proto(cls, proto: proto.Message):
        return cls(core.ID.from_bytes(proto.findValue.key))
