
from core import ID, Node
import messages as msg

def test_parsing():
    node = Node('localhost', 3000, ID(10))
    message = msg.FindNode(ID(10)).finalize(node)
    parsed = msg.Message.parse_protobuf(message)
    assert parsed.key == ID(10)
