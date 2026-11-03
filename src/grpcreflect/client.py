"""Client-side gRPC Server Reflection, with automatic v1 -> v1alpha fallback.

Modeled on Go's `Client`/`ClientStream` (`grpcreflect-go/client.go`): a
`ClientStream` pins a sequence of reflection calls to one open stream (so
they're all answered by a single, unchanging view of the server's schema),
and `Client` transparently falls back from v1 to v1alpha the first time a
v1 stream reports `UNIMPLEMENTED` -- mirroring Go's `v1unimplemented
atomic.Bool` + `shouldRetryLocked`, except the retry is scoped to the one
request that discovered the fallback, same as Go's `send()` loop.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf import Oneof, merge_from_binary
from protobuf.wkt import FileDescriptorProto

from grpcreflect._gen.grpc.reflection.v1 import reflection_connect as _v1_connect
from grpcreflect._gen.grpc.reflection.v1 import reflection_pb as _v1_pb
from grpcreflect._gen.grpc.reflection.v1alpha import (
    reflection_connect as _v1alpha_connect,
)
from grpcreflect._gen.grpc.reflection.v1alpha import reflection_pb as _v1alpha_pb

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from connectrpc.request import Headers

REQ = TypeVar("REQ")
RESP = TypeVar("RESP")

# ErrorResponse.error_code carries a grpc.StatusCode value (see reflection.proto);
# Connect's `Code` enum is numbered 1-16 to match those exact codes, so this table
# is just that numbering made explicit (mirrors Go's `connect.Code(errResp.ErrorCode)`).
_CODE_BY_GRPC_STATUS: tuple[Code, ...] = (
    Code.CANCELED,
    Code.UNKNOWN,
    Code.INVALID_ARGUMENT,
    Code.DEADLINE_EXCEEDED,
    Code.NOT_FOUND,
    Code.ALREADY_EXISTS,
    Code.PERMISSION_DENIED,
    Code.RESOURCE_EXHAUSTED,
    Code.FAILED_PRECONDITION,
    Code.ABORTED,
    Code.OUT_OF_RANGE,
    Code.UNIMPLEMENTED,
    Code.INTERNAL,
    Code.UNAVAILABLE,
    Code.DATA_LOSS,
    Code.UNAUTHENTICATED,
)


def _code_from_grpc_status(status: int) -> Code:
    if 1 <= status <= len(_CODE_BY_GRPC_STATUS):
        return _CODE_BY_GRPC_STATUS[status - 1]
    return Code.INTERNAL


@dataclass(frozen=True, slots=True)
class _ClientSchema(Generic[REQ, RESP]):
    """The generated request/response classes needed to speak one protocol version."""

    request: type[REQ]
    extension_request: type
    response: type[RESP]


_SCHEMA_V1 = _ClientSchema(
    request=_v1_pb.ServerReflectionRequest,
    extension_request=_v1_pb.ExtensionRequest,
    response=_v1_pb.ServerReflectionResponse,
)

_SCHEMA_V1ALPHA = _ClientSchema(
    request=_v1alpha_pb.ServerReflectionRequest,
    extension_request=_v1alpha_pb.ExtensionRequest,
    response=_v1alpha_pb.ServerReflectionResponse,
)


def _unwrap(response: Any, field: str) -> Any:
    """Extracts `field` from `response.message_response`, or raises a protocol error.

    Mirrors Go's `errWrongResponseType`: a plain `RuntimeError`, not a
    `ConnectError`, since a wrong-shaped-but-successful reply is a server bug,
    not a well-formed RPC failure a caller should branch on by status code.
    """
    message_response = response.message_response
    if message_response is None or message_response.field != field:
        actual = message_response.field if message_response is not None else None
        msg = f"protocol error: wrong response type {actual!r} in reply (expected {field!r})"
        raise RuntimeError(msg)
    return message_response.value


def _parse_file_descriptors(data: list[bytes]) -> list[FileDescriptorProto]:
    descriptors = []
    for blob in data:
        descriptor = FileDescriptorProto()
        try:
            merge_from_binary(descriptor, blob)
        except Exception as error:
            msg = f"reply contained invalid descriptor proto: {error}"
            raise RuntimeError(msg) from error
        descriptors.append(descriptor)
    return descriptors


class ClientStream:
    """One `ServerReflectionInfo` stream, answering a sequence of queries.

    The protocol carries no request-correlation ID, so replies must be read
    back in the same order requests were sent; `_send()` enforces exactly one
    request in flight at a time via `_lock`, reproducing the guarantee Go's
    mutex-guarded `send()` gives for free. Not safe to share across
    concurrent callers beyond that serialization -- create one `ClientStream`
    per logical sequence of reflection calls.
    """

    def __init__(
        self,
        client: Client,
        *,
        host: str = "",
        headers: Headers | None = None,
    ) -> None:
        self._client = client
        self._host = host
        self._headers = headers
        self._lock = asyncio.Lock()
        self._closed = False
        self._is_v1 = not client._v1_unimplemented  # noqa: SLF001
        self._requests: asyncio.Queue[Any] = asyncio.Queue()
        self._responses = self._open_stream()

    def _schema(self) -> _ClientSchema[Any, Any]:
        return _SCHEMA_V1 if self._is_v1 else _SCHEMA_V1ALPHA

    def _open_stream(self) -> AsyncIterator[Any]:
        client = (
            self._client._v1_client if self._is_v1 else self._client._v1alpha_client
        )  # noqa: SLF001
        return client.server_reflection_info(
            self._iter_requests(), headers=self._headers
        )

    async def _iter_requests(self) -> AsyncIterator[Any]:
        while (request := await self._requests.get()) is not None:
            yield request

    async def _send(self, message_request: Oneof[str, object]) -> Any:
        if self._closed:
            msg = "ClientStream is closed"
            raise RuntimeError(msg)
        async with self._lock:
            while True:
                schema = self._schema()
                request = schema.request(
                    host=self._host, message_request=message_request
                )
                try:
                    await self._requests.put(request)
                    response = await anext(self._responses)
                except ConnectError as error:
                    if self._is_v1 and error.code is Code.UNIMPLEMENTED:
                        self._client._v1_unimplemented = True  # noqa: SLF001
                        self._is_v1 = False
                        self._requests = asyncio.Queue()
                        self._responses = self._open_stream()
                        continue
                    raise
                message_response = response.message_response
                if (
                    isinstance(message_response, Oneof)
                    and message_response.field == "error_response"
                ):
                    error_response = message_response.value
                    code = _code_from_grpc_status(error_response.error_code)
                    raise ConnectError(code, error_response.error_message)
                return response

    async def list_services(self) -> list[str]:
        response = await self._send(Oneof(field="list_services", value=""))
        return [
            service.name
            for service in _unwrap(response, "list_services_response").service
        ]

    async def file_by_filename(self, name: str) -> list[FileDescriptorProto]:
        response = await self._send(Oneof(field="file_by_filename", value=name))
        return _parse_file_descriptors(
            _unwrap(response, "file_descriptor_response").file_descriptor_proto
        )

    async def file_containing_symbol(self, name: str) -> list[FileDescriptorProto]:
        response = await self._send(Oneof(field="file_containing_symbol", value=name))
        return _parse_file_descriptors(
            _unwrap(response, "file_descriptor_response").file_descriptor_proto
        )

    async def file_containing_extension(
        self, containing_type: str, extension_number: int
    ) -> list[FileDescriptorProto]:
        extension_request = self._schema().extension_request(
            containing_type=containing_type, extension_number=extension_number
        )
        response = await self._send(
            Oneof(field="file_containing_extension", value=extension_request)
        )
        return _parse_file_descriptors(
            _unwrap(response, "file_descriptor_response").file_descriptor_proto
        )

    async def all_extension_numbers_of_type(self, name: str) -> list[int]:
        response = await self._send(
            Oneof(field="all_extension_numbers_of_type", value=name)
        )
        return list(
            _unwrap(response, "all_extension_numbers_response").extension_number
        )

    async def close(self) -> None:
        """Half-closes the stream, then drains it until the server signals EOF."""
        if self._closed:
            return
        self._closed = True
        async with self._lock:
            await self._requests.put(None)
            async for _ in self._responses:
                msg = "protocol error: server sent unexpected response after half-close"
                raise RuntimeError(msg)

    async def __aenter__(self) -> ClientStream:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()


class Client:
    """Speaks gRPC Server Reflection against one server, preferring v1.

    Falls back to v1alpha the first time a stream discovers v1 is
    `UNIMPLEMENTED`; once that happens, every later `new_stream()` goes
    straight to v1alpha (mirrors Go's `Client`/`v1unimplemented atomic.Bool`).
    Accepts the same keyword options as `connectrpc.client.ConnectClient`.
    """

    def __init__(self, address: str, **client_options: Any) -> None:
        self._v1_client = _v1_connect.ServerReflectionClient(address, **client_options)
        self._v1alpha_client = _v1alpha_connect.ServerReflectionClient(
            address, **client_options
        )
        self._v1_unimplemented = False

    def new_stream(
        self, *, host: str = "", headers: Headers | None = None
    ) -> ClientStream:
        """Opens a new `ClientStream` for a sequence of reflection calls."""
        return ClientStream(self, host=host, headers=headers)

    async def close(self) -> None:
        await self._v1_client.close()
        await self._v1alpha_client.close()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
