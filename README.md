# kademlia
An implementation of Kademlia (S/Kademlia eventually)

# Usage

```python
import asyncio

async def main():
    first = Node('localhost', 9000)
    second = Node('localhost', 9001)
    third = Node('localhost', 9002)

    # each of these calls starts a server
    first.listen()
    second.listen()
    third.listen()

    await second.bootstrap('localhost', 9000)
    await third.bootstrap('localhost', 9000)

asyncio.run(main())
```
