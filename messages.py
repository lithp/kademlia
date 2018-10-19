import dataclasses
import google.protobuf
import typing

import core
import protobuf.rpc_pb2 as proto


@dataclasses.dataclass
class Message:
    message_types: typing.ClassVar = dict()
    nonce: bytes = dataclasses.field(init=False, default_factory=core.newnonce)
    _message: proto.Message = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        self._message = proto.Message()

    def __init_subclass__(cls, **kwargs):
        if hasattr(cls, 'field'):
            Message.message_types[cls.field] = cls
        super().__init_subclass__(**kwargs)

    def finalize(self, node: core.Node):
        message = self._message
        message.nonce = self.nonce
        message.sender.ip = node.addr
        message.sender.port = node.port
        message.sender.nodeid = node.nodeid.to_bytes()
        return message

    @classmethod
    def parse_protobuf(cls, protobuf: proto.Message):
        for fieldName, fieldClass in cls.message_types.items():
            if protobuf.HasField(fieldName):
                result = fieldClass.from_proto(protobuf)
                result.nonce = protobuf.nonce
                return result
        raise ValueError(f'did not recognize {proto}')

@dataclasses.dataclass
class FindNode(Message):
    field = 'findNode'
    key: core.ID

    def __post_init__(self):
        super().__post_init__()
        self._message.findNode.key = self.key.to_bytes()

    @classmethod
    def from_proto(cls, proto: proto.Message):
        return cls(key=core.ID.from_bytes(proto.findNode.key))

class Response(Message):
    def __init__(self, nonce: bytes):
        super().__init__()
        self.nonce = nonce

class FindNodeResponse(Response):
    def __init__(self, nonce, nodes: typing.List[core.Node]):
        self.nodes = nodes
        super().__init__(nonce)

    def __post_init__(self):
        super().__post_init__()
        for node in self.nodes:
            neighbor = self._message.findNodeResponse.neighbors.add()
            neighbor.ip = node.addr
            neighbor.port = node.port
            neighbor.nodeid = node.nodeid.to_bytes()

    @classmethod
    def parse(message):
        self.nodes = [
            core.Node(
                addr=neighbor.ip,
                port=neighbor.port,
                nodeid=read_nodeid(neighbor.nodeid)
            ) for neighbor in message.findNodeResponse.neighbors
        ]

# if there was ever a time for metaclasses

class Ping(Message):
    field = 'ping'

    def __init__(self):
        super().__init__()
        self._message.ping.SetInParent()

class Pong(Response):
    field = 'pong'

    def __init__(self, nonce):
        super().__init__(nonce)
        self._message.pong.SetInParent()

class StoreResponse(Response):
    field = 'storeResponse'

    def __init__(self, nonce):
        super().__init__(nonce)
        self._message.storeResponse.SetInParent()

class Store(Message):
    field = 'store'

    def __init__(self, key: core.ID, value: bytes):
        super().__init__()
        self._message.store.key = key.to_bytes()
        self._message.store.value = value

class FoundValue(Response):
    field = 'foundValue'

    def __init__(self, nonce, key: core.ID, value: bytes):
        super().__init__(nonce)
        self._message.foundValue.key = key.to_bytes()
        self._message.foundValue.value = value

class FindValue(Message):
    field = 'findValue'

    def __init__(self, key: core.ID):
        super().__init__()
        self._message.findValue.key = key.to_bytes()
