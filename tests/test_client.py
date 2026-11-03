"""End-to-end tests for `client.py`, driven against a real ASGI server.

Mirrors grpcreflect-go's resolver test suite (docs/implementation-plan.md
§8/§9 build order item 4): a real pyvoy server, hit with the real
`Client`/`ClientStream` over real HTTP, taking over from the `grpcurl` smoke
test used to validate `server.py`'s wire-level behavior.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import pyvoy
from connectrpc.code import Code
from connectrpc.errors import ConnectError

from grpcreflect.client import Client

_V1_REFLECTION = "grpc.reflection.v1.ServerReflection"
_V1ALPHA_REFLECTION = "grpc.reflection.v1alpha.ServerReflection"


async def _address_for(app_path: str) -> AsyncIterator[str]:
    server = pyvoy.PyvoyServer(app_path, port=0, log_level="error")
    await server.start()
    try:
        yield f"http://127.0.0.1:{server.listener_port}"
    finally:
        await server.stop()


@pytest_asyncio.fixture
async def address() -> AsyncIterator[str]:
    async for addr in _address_for("_e2e_apps:app_all"):
        yield addr


@pytest_asyncio.fixture
async def v1alpha_only_address() -> AsyncIterator[str]:
    async for addr in _address_for("_e2e_apps:app_v1alpha_only"):
        yield addr


async def test_list_services_includes_both_reflection_services(address: str) -> None:
    async with Client(address) as client, client.new_stream() as stream:
        services = await stream.list_services()
    assert _V1_REFLECTION in services
    assert _V1ALPHA_REFLECTION in services


async def test_file_containing_symbol_resolves_real_upstream_file(address: str) -> None:
    async with Client(address) as client, client.new_stream() as stream:
        files = await stream.file_containing_symbol(_V1_REFLECTION)
    assert [f.name for f in files] == ["grpc/reflection/v1/reflection.proto"]


async def test_file_by_filename_round_trips(address: str) -> None:
    async with Client(address) as client, client.new_stream() as stream:
        files = await stream.file_by_filename(
            "grpc/reflection/v1alpha/reflection.proto"
        )
    assert [f.name for f in files] == ["grpc/reflection/v1alpha/reflection.proto"]


async def test_file_by_filename_not_found_raises(address: str) -> None:
    async with Client(address) as client, client.new_stream() as stream:
        with pytest.raises(ConnectError) as exc_info:
            await stream.file_by_filename("does/not/exist.proto")
    assert exc_info.value.code is Code.NOT_FOUND


async def test_falls_back_to_v1alpha_when_v1_is_unimplemented(
    v1alpha_only_address: str,
) -> None:
    async with Client(v1alpha_only_address) as client, client.new_stream() as stream:
        services = await stream.list_services()
    assert _V1ALPHA_REFLECTION in services
