"""Version-agnostic core of the gRPC Server Reflection protocol.

Both `grpc.reflection.v1` and `grpc.reflection.v1alpha` describe the exact
same wire format (only the package name differs), so `Reflector` implements
the protocol once, against a small `ReflectionSchema` describing which
generated message classes to construct for a given version. `server.py`
supplies one `ReflectionSchema` per version.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf import (
    DescEnum,
    DescExtension,
    DescFile,
    DescMessage,
    DescService,
    Oneof,
    Registry,
)

from grpcreflect._gen.grpc.reflection.v1 import reflection_pb as _reflection_pb_v1
from grpcreflect._gen.grpc.reflection.v1alpha import (
    reflection_pb as _reflection_pb_v1alpha,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from connectrpc.request import RequestContext

# The gRPC Server Reflection protocol re-uses grpc.StatusCode values for its
# in-band ErrorResponse.error_code (see reflection.proto), independent of
# whatever RPC-level status the surrounding Connect call itself completes
# with. NOT_FOUND is the only code the protocol ever needs to send this way.
_NOT_FOUND_STATUS_CODE = 5


class _RequestLike(Protocol):
    """The shape `Reflector` needs from a `ServerReflectionRequest`.

    v1 and v1alpha each generate their own concrete `ServerReflectionRequest`
    class; this `Protocol` lets `Reflector` accept either one structurally,
    the same way a Go interface is satisfied by any type with the right
    methods, without either class inheriting from a common base.
    """

    host: str
    message_request: Oneof[str, object] | None


REQ = TypeVar("REQ", bound=_RequestLike)
RESP = TypeVar("RESP")


@dataclass(frozen=True, slots=True)
class ReflectionSchema(Generic[REQ, RESP]):
    """The generated message classes needed to speak one protocol version.

    `response` is typed so `Reflector.serve()` can be declared to return
    `AsyncIterator[RESP]`; the remaining fields are only ever constructed
    (never returned to callers), so they're left loosely typed.
    """

    response: type[RESP]
    file_descriptor_response: type
    extension_number_response: type
    list_service_response: type
    service_response: type
    error_response: type


@dataclass(frozen=True, slots=True)
class ReflectorOptions:
    """Options controlling how a `Reflector` answers queries.

    Attributes:
        extension_registry: Where to look up extensions (`file_containing_extension`,
            `all_extension_numbers_of_type`). Defaults to the main registry passed to
            `Reflector`. Mirrors Go's `ExtensionResolver` option, for callers who track
            extensions separately from the rest of their schema.
    """

    extension_registry: Registry | None = None


class Reflector(Generic[REQ, RESP]):
    """Implements gRPC Server Reflection queries against a `Registry`.

    Unlike Go's `Reflector`, which draws on protobuf-go's single global
    registry and therefore needs a `Namer` to say which of the many
    compiled-in services should be exposed, this `Reflector` treats
    whatever `Registry` it's given as the complete, authoritative answer:
    every file, message, enum, extension, and service the caller folded
    into it is exactly what gets reflected, including `ListServices`, which
    simply enumerates the `DescService` entries present.
    """

    def __init__(
        self,
        registry: Registry,
        schema: ReflectionSchema[REQ, RESP],
        *,
        options: ReflectorOptions | None = None,
    ) -> None:
        # Fold in the authentic reflection descriptors themselves, so queries
        # like FileContainingSymbol("grpc.reflection.v1.ServerReflection")
        # resolve even if the caller's own registry doesn't include them.
        self._registry = Registry(
            registry, _reflection_pb_v1.desc(), _reflection_pb_v1alpha.desc()
        )
        self._schema = schema
        resolved_options = options or ReflectorOptions()
        self._extension_registry = resolved_options.extension_registry or self._registry

    async def serve(
        self,
        requests: AsyncIterator[REQ],
        ctx: RequestContext[REQ, RESP],
    ) -> AsyncIterator[RESP]:
        """Answers a stream of `ServerReflectionRequest`s.

        A single stream may carry many requests; `sent` tracks which files
        have already been serialized on *this* stream so that dependency
        closures shared across requests aren't repeated (mirrors Go's
        per-call `fileDescriptorNameSet`).
        """
        del ctx
        schema = self._schema
        sent: set[str] = set()
        async for request in requests:
            message_response = self._dispatch(request.message_request, sent)
            yield schema.response(
                valid_host=request.host,
                original_request=request,
                message_response=message_response,
            )

    def _dispatch(
        self, message_request: Oneof[str, object] | None, sent: set[str]
    ) -> Oneof[str, object]:
        match message_request:
            case Oneof(field="file_by_filename", value=str() as filename):
                return self._file_response(self._registry.file(filename), sent)
            case Oneof(field="file_containing_symbol", value=str() as symbol):
                found = self._find_symbol(symbol)
                return self._file_response(found.file if found else None, sent)
            case Oneof(field="file_containing_extension", value=extension_request):
                extension = self._extension_registry.extension_for(
                    extension_request.containing_type,
                    extension_request.extension_number,
                )
                return self._file_response(extension.file if extension else None, sent)
            case Oneof(field="all_extension_numbers_of_type", value=str() as type_name):
                return self._all_extension_numbers(type_name)
            case Oneof(field="list_services"):
                return self._list_services()
            case _:
                raise ConnectError(
                    Code.INVALID_ARGUMENT,
                    f"invalid message_request: {message_request!r}",
                )

    def _find_symbol(
        self, name: str
    ) -> DescMessage | DescEnum | DescService | DescExtension | None:
        registry = self._registry
        return (
            registry.message(name)
            or registry.enum(name)
            or registry.service(name)
            or registry.extension(name)
        )

    def _file_response(
        self, file: DescFile | None, sent: set[str]
    ) -> Oneof[str, object]:
        schema = self._schema
        if file is None:
            return _not_found(schema)
        data = _file_descriptor_with_dependencies(file, sent)
        return Oneof(
            field="file_descriptor_response",
            value=schema.file_descriptor_response(file_descriptor_proto=data),
        )

    def _all_extension_numbers(self, type_name: str) -> Oneof[str, object]:
        schema = self._schema
        extensions = self._extension_registry.extensions_for(type_name)
        if not extensions and self._find_symbol(type_name) is None:
            return _not_found(schema)
        numbers = sorted(extension.number for extension in extensions)
        return Oneof(
            field="all_extension_numbers_response",
            value=schema.extension_number_response(
                base_type_name=type_name, extension_number=numbers
            ),
        )

    def _list_services(self) -> Oneof[str, object]:
        schema = self._schema
        services = [
            schema.service_response(name=item.type_name)
            for item in self._registry
            if isinstance(item, DescService)
        ]
        return Oneof(
            field="list_services_response",
            value=schema.list_service_response(service=services),
        )


def _not_found(
    schema: ReflectionSchema, message: str = "not found"
) -> Oneof[str, object]:
    return Oneof(
        field="error_response",
        value=schema.error_response(
            error_code=_NOT_FOUND_STATUS_CODE, error_message=message
        ),
    )


def _file_descriptor_with_dependencies(root: DescFile, sent: set[str]) -> list[bytes]:
    """BFS over `root` and its transitive imports, serializing each file once.

    The root file is always included, even if a prior request on the same
    stream already sent it; only its *dependencies* are skipped when
    already-sent, matching Go's `fileDescriptorWithDependencies`.
    """
    results: list[bytes] = []
    visited: set[str] = set()
    queue: deque[DescFile] = deque((root,))
    while queue:
        current = queue.popleft()
        if current.name in visited:
            continue
        visited.add(current.name)
        if not results or current.name not in sent:
            sent.add(current.name)
            results.append(current.proto.to_binary())
        queue.extend(current.dependencies)
    return results
