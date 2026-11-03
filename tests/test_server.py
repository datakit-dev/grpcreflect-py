"""Unit tests for the v1 / v1alpha adapter layer and
`mount_reflection_handlers()` (see docs/implementation-plan.md §5, §9 step 3).

`_mounted_service_desc()`'s generic-subclass introspection is exercised with
a throwaway `ConnectASGIApplication` subclass built directly against the real
generated `ServerReflection` service Protocol -- there's no need for a
separate example `.proto`/service just to prove the mechanism works.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from connectrpc.server import ConnectASGIApplication
from protobuf import Oneof

from grpcreflect._gen.grpc.reflection.v1 import reflection_connect as v1_connect
from grpcreflect._gen.grpc.reflection.v1 import reflection_pb as v1
from grpcreflect._gen.grpc.reflection.v1alpha import reflection_pb as v1alpha
from grpcreflect.server import (
    ReflectionServiceV1,
    ReflectionServiceV1Alpha,
    _mounted_service_desc,
    mount_reflection_handlers,
)


class _FakeMountedApp(ConnectASGIApplication[v1_connect.ServerReflection]):
    """Stands in for a caller's own generated `XxxASGIApplication`.

    Built against the real `ServerReflection` Protocol class purely because
    it's a real, already-imported generated service -- `_mounted_service_desc`
    only cares about the generic base, not what the service actually does.
    """

    def __init__(self, service: v1_connect.ServerReflection) -> None:
        super().__init__(service=service, endpoints=lambda _svc: {})

    @property
    def path(self) -> str:
        return "/fake"


class _UnimplementedService:
    def server_reflection_info(self, request: object, ctx: object, /) -> object:
        raise NotImplementedError

    @classmethod
    def desc(cls) -> object:
        return v1_connect.ServerReflection.desc()


def test_mounted_service_desc_recovers_descriptor_from_instance() -> None:
    app = _FakeMountedApp(_UnimplementedService())
    assert _mounted_service_desc(app) is v1_connect.ServerReflection.desc()


def test_mounted_service_desc_rejects_non_connect_app() -> None:
    with pytest.raises(TypeError, match="ConnectASGIApplication"):
        _mounted_service_desc(object())


def test_reflection_service_v1_desc() -> None:
    assert ReflectionServiceV1.desc() is v1_connect.ServerReflection.desc()


def test_mount_reflection_handlers_returns_apps_then_reflection_apps() -> None:
    fake_app = _FakeMountedApp(_UnimplementedService())
    apps = mount_reflection_handlers(fake_app)
    assert len(apps) == 3
    fake, v1_app, v1alpha_app = apps
    assert fake is fake_app
    assert v1_app.path == "/grpc.reflection.v1.ServerReflection"
    assert v1alpha_app.path == "/grpc.reflection.v1alpha.ServerReflection"


async def test_mount_reflection_handlers_registry_includes_mounted_service() -> None:
    _fake_app, v1_app, v1alpha_app = mount_reflection_handlers(
        _FakeMountedApp(_UnimplementedService())
    )

    # `_service` is a private attribute of `ConnectASGIApplication`; reaching
    # into it here is a test-only convenience to drive the wrapped
    # `ReflectionServiceV1`/`ReflectionServiceV1Alpha` directly without
    # standing up a real ASGI/HTTP server (see the §9 step-3 `grpcurl` smoke
    # test for true wire-level coverage).
    v1_service: ReflectionServiceV1 = v1_app._service  # noqa: SLF001
    v1alpha_service: ReflectionServiceV1Alpha = v1alpha_app._service  # noqa: SLF001

    async def requests_v1() -> AsyncIterator[v1.ServerReflectionRequest]:
        yield v1.ServerReflectionRequest(
            message_request=Oneof(field="list_services", value="")
        )

    responses = [
        resp async for resp in v1_service.server_reflection_info(requests_v1(), None)
    ]
    names = {service.name for service in responses[0].message_response.value.service}
    assert names == {
        "grpc.reflection.v1.ServerReflection",
        "grpc.reflection.v1alpha.ServerReflection",
    }

    async def requests_v1alpha() -> AsyncIterator[v1alpha.ServerReflectionRequest]:
        yield v1alpha.ServerReflectionRequest(
            message_request=Oneof(field="list_services", value="")
        )

    responses = [
        resp
        async for resp in v1alpha_service.server_reflection_info(
            requests_v1alpha(), None
        )
    ]
    names = {service.name for service in responses[0].message_response.value.service}
    assert names == {
        "grpc.reflection.v1.ServerReflection",
        "grpc.reflection.v1alpha.ServerReflection",
    }
