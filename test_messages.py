
from core import ID, Node
import messages as msg

def test_parsing():
    node = Node('localhost', 3000, ID(10))
    message = msg.FindNode(ID(10)).finalize(node)
    parsed = msg.Message.parse_protobuf(message)
    assert parsed.key == ID(10)

def test_parse_find_node_response():
    nodes = [Node(addr='localhost', port=i, nodeid=ID(i)) for i in range(5)]

    find_node_response = msg.FindNodeResponse(b'', nodes[0:]).finalize(nodes[0])
    parsed_nodes = msg.Message.parse_protobuf(find_node_response)

    assert parsed_nodes.nodes == nodes[0:]
