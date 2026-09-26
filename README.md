# grpcreflect-py

An idiomatic Python implementation of the [gRPC Server Reflection
protocol][grpc-reflection] (`grpc.reflection.v1` and
`grpc.reflection.v1alpha`), built on [`protobuf-py`][protobuf-py] and
[`connectrpc`][connect-python]. Modeled on Buf's Go reference
implementation, [`grpcreflect-go`][grpcreflect-go].

Since the reflection services here are just `ConnectASGIApplication`s like
any other, they work over whichever of `connectrpc`'s protocols your own
services already speak -- Connect, gRPC, and gRPC-Web -- with no extra
configuration.

[grpc-reflection]: https://github.com/grpc/grpc/blob/master/doc/server-reflection.md
[protobuf-py]: https://github.com/bufbuild/protobuf-py
[connect-python]: https://github.com/connectrpc/connect-py
[grpcreflect-go]: https://github.com/connectrpc/grpcreflect-go

## Install

```sh
uv add grpcreflect-py
```

## Usage

`mount_reflection_handlers()` takes the same `ConnectASGIApplication`
instances you're already mounting for your own services, derives a registry
from their descriptors, and returns them alongside ready-to-mount v1 and
v1alpha reflection applications:

```python
from starlette.applications import Starlette
from starlette.routing import Mount
from grpcreflect.server import mount_reflection_handlers

from example.greet import GreetServiceImpl
from example.gen.greet.v1.greet_connect import GreetServiceASGIApplication

app = Starlette(
    routes=[
        Mount(a.path, app=a)
        for a in mount_reflection_handlers(
            GreetServiceASGIApplication(GreetServiceImpl())
        )
    ]
)
```

`ServerReflectionInfo` is a bidirectional streaming RPC, which requires an
HTTP/2-capable ASGI server. As of this writing, [`pyvoy`][pyvoy] is the
server most reliably passing the Connect conformance suite's HTTP/2 and
bidi-streaming tests, so it's the recommended way to serve `app` above:

```python
import asyncio
import pyvoy

async def main() -> None:
    server = pyvoy.PyvoyServer("myapp:app", port=8080)
    await server.start()
    await server.wait()

asyncio.run(main())
```

You aren't required to use `pyvoy` -- any ASGI server with working HTTP/2
and bidi-streaming support will do -- but not every ASGI server reliably
supports both today.

[pyvoy]: https://github.com/curioswitch/pyvoy

## Client

`Client` speaks gRPC Server Reflection against a server, preferring v1 and
falling back to v1alpha automatically the first time it discovers v1 isn't
implemented. `new_stream()` opens a `ClientStream`, which pins a sequence of
calls to one open stream so they're all answered by a single, consistent
view of the server's schema:

```python
import asyncio

from grpcreflect.client import Client


async def main() -> None:
    async with Client("http://localhost:8080") as client, client.new_stream() as stream:
        print(await stream.list_services())

        for descriptor in await stream.file_containing_symbol("greet.v1.GreetService"):
            print(descriptor.name)


asyncio.run(main())
```

## Development

```sh
uv run -- buf generate   # regenerate src/grpcreflect/_gen from the upstream schema
uv run pytest
```
