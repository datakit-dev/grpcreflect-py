"""v1 / v1alpha adapter services + a `mount_reflection_handlers()` factory.

Both generated `ServerReflection` services describe the exact same wire
format, so each adapter here just forwards to a shared `Reflector` core
(`reflector.py`), one instance per protocol version since each version's
generated response classes are distinct, non-inheriting types.
"""

from __future__ import annotations

import typing
from typing import TYPE_CHECKING, Any

from connectrpc.server import DEFAULT_READ_MAX_BYTES, ConnectASGIApplication
from protobuf import Registry

from grpcreflect._gen.grpc.reflection.v1 import reflection_connect as _v1_connect
from grpcreflect._gen.grpc.reflection.v1 import reflection_pb as _v1_pb
from grpcreflect._gen.grpc.reflection.v1alpha import (
    reflection_connect as _v1alpha_connect,
)
from grpcreflect._gen.grpc.reflection.v1alpha import reflection_pb as _v1alpha_pb
from grpcreflect.reflector import Reflector, ReflectionSchema, ReflectorOptions

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from connectrpc.codec import Codec
    from connectrpc.compression import Compression
    from connectrpc.interceptor import Interceptor
    from connectrpc.request import RequestContext
    from protobuf import DescService

_SCHEMA_V1 = ReflectionSchema(
    response=_v1_pb.ServerReflectionResponse,
    file_descriptor_response=_v1_pb.FileDescriptorResponse,
    extension_number_response=_v1_pb.ExtensionNumberResponse,
    list_service_response=_v1_pb.ListServiceResponse,
    service_response=_v1_pb.ServiceResponse,
    error_response=_v1_pb.ErrorResponse,
)

_SCHEMA_V1ALPHA = ReflectionSchema(
    response=_v1alpha_pb.ServerReflectionResponse,
    file_descriptor_response=_v1alpha_pb.FileDescriptorResponse,
    extension_number_response=_v1alpha_pb.ExtensionNumberResponse,
    list_service_response=_v1alpha_pb.ListServiceResponse,
    service_response=_v1alpha_pb.ServiceResponse,
    error_response=_v1alpha_pb.ErrorResponse,
)


class ReflectionServiceV1:
    """Implements `grpc.reflection.v1.ServerReflection` over a `Reflector`."""

    def __init__(
        self,
        reflector: Reflector[
            _v1_pb.ServerReflectionRequest, _v1_pb.ServerReflectionResponse
        ],
    ) -> None:
        self._reflector = reflector

    def server_reflection_info(
        self,
        request: AsyncIterator[_v1_pb.ServerReflectionRequest],
        ctx: RequestContext[
            _v1_pb.ServerReflectionRequest, _v1_pb.ServerReflectionResponse
        ],
        /,
    ) -> AsyncIterator[_v1_pb.ServerReflectionResponse]:
        return self._reflector.serve(request, ctx)

    @classmethod
    def desc(cls) -> DescService:
        return _v1_connect.ServerReflection.desc()


class ReflectionServiceV1Alpha:
    """Implements `grpc.reflection.v1alpha.ServerReflection` over a `Reflector`."""

    def __init__(
        self,
        reflector: Reflector[
            _v1alpha_pb.ServerReflectionRequest, _v1alpha_pb.ServerReflectionResponse
        ],
    ) -> None:
        self._reflector = reflector

    def server_reflection_info(
        self,
        request: AsyncIterator[_v1alpha_pb.ServerReflectionRequest],
        ctx: RequestContext[
            _v1alpha_pb.ServerReflectionRequest, _v1alpha_pb.ServerReflectionResponse
        ],
        /,
    ) -> AsyncIterator[_v1alpha_pb.ServerReflectionResponse]:
        return self._reflector.serve(request, ctx)

    @classmethod
    def desc(cls) -> DescService:
        return _v1alpha_connect.ServerReflection.desc()


def _mounted_service_desc(app: ConnectASGIApplication[Any]) -> DescService:
    """Recovers the `DescService` a generated `ConnectASGIApplication` was built for.

    Every `protoc-gen-connectrpc`-generated `XxxASGIApplication` subclasses
    `ConnectASGIApplication[XxxService]`; Python's typing machinery records
    that generic argument on the class itself (`__orig_bases__`), which lets
    us recover `XxxService` -- and from it, its `desc()` classmethod --
    without touching any of the application instance's own private state
    (which, for lifespan-provided services, may not even be resolved yet).
    """
    for base in getattr(type(app), "__orig_bases__", ()):
        if typing.get_origin(base) is ConnectASGIApplication:
            (service_class,) = typing.get_args(base)
            return service_class.desc()
    msg = (
        f"{type(app).__name__} does not appear to be a generated "
        "ConnectASGIApplication subclass (no ConnectASGIApplication[...] base found)"
    )
    raise TypeError(msg)


def mount_reflection_handlers(
    *apps: ConnectASGIApplication[Any],
    registry: Registry | None = None,
    options: ReflectorOptions | None = None,
    interceptors: Iterable[Interceptor] = (),
    read_max_bytes: int | None = DEFAULT_READ_MAX_BYTES,
    compressions: Iterable[Compression] | None = None,
    codecs: Iterable[Codec] | None = None,
) -> tuple[ConnectASGIApplication[Any], ...]:
    """Builds v1/v1alpha reflection ASGI applications for `*apps`.

    Args:
        *apps: The same generated `ConnectASGIApplication` instances you're
            mounting for your own services -- their descriptors are folded
            into the registry the returned reflection applications answer
            queries against, so there's no separate `Registry` to build by
            hand for the common case.
        registry: Additional descriptors to expose that aren't represented
            by one of `*apps` (for example, a file with no Connect service).
        options: See `ReflectorOptions`.
        interceptors: Forwarded to the v1 and v1alpha `ConnectASGIApplication`
            constructors, same as you'd pass to one of your own generated
            applications.
        read_max_bytes: Forwarded to the v1 and v1alpha `ConnectASGIApplication`
            constructors.
        compressions: Forwarded to the v1 and v1alpha `ConnectASGIApplication`
            constructors.
        codecs: Forwarded to the v1 and v1alpha `ConnectASGIApplication`
            constructors.

    Returns:
        `*apps` followed by the v1 and v1alpha reflection applications, so
        the result can be mounted directly at each app's own `.path`:

        ```python
        app = Starlette(
            routes=[
                Mount(a.path, app=a)
                for a in mount_reflection_handlers(greeter, other_service)
            ]
        )
        ```
    """
    combined = Registry(registry) if registry is not None else Registry()
    for app in apps:
        combined.add(_mounted_service_desc(app).file)

    v1_reflector = Reflector(combined, _SCHEMA_V1, options=options)
    v1alpha_reflector = Reflector(combined, _SCHEMA_V1ALPHA, options=options)

    v1_app = _v1_connect.ServerReflectionASGIApplication(
        ReflectionServiceV1(v1_reflector),
        interceptors=interceptors,
        read_max_bytes=read_max_bytes,
        compressions=compressions,
        codecs=codecs,
    )
    v1alpha_app = _v1alpha_connect.ServerReflectionASGIApplication(
        ReflectionServiceV1Alpha(v1alpha_reflector),
        interceptors=interceptors,
        read_max_bytes=read_max_bytes,
        compressions=compressions,
        codecs=codecs,
    )
    return (*apps, v1_app, v1alpha_app)
