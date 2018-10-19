# kademlia
An implementation of Kademlia

# Usage

```python
import asyncio

async def main():
    first = Node('localhost', 9000)
    second = Node('localhost', 9001)
    third = Node('localhost', 9002)

    # bind the servers to their ports
    await first.listen()
    await second.listen()
    await third.listen()

    # them them about each other
    await first.bootstrap('localhost', 9001)
    await second.bootstrap('localhost', 9002)

    # now that they're all talking to each other:
    await first.store_value(0b100, b'hello')
    value = await third.find_value(0b100)
    assert value == b'hello'

asyncio.run(main())
```
