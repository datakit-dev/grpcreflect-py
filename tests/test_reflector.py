"""Unit tests for `Reflector.serve()`, run in-process against hand-built
requests -- no ASGI/HTTP involved (see docs/implementation-plan.md §8).

Extension-related tests use real extensions shipped by `protobuf-py` itself
(the `google.protobuf.FeatureSet` language-specific feature extensions, and
`google.protobuf.EnumValueOptions`'s `ext_json`) rather than hand-rolled
fixtures, since those are genuine `DescExtension` objects backed by real
generated code.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import protobuf.wkt as wkt
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from protobuf import Oneof, Registry
from protobuf.wkt import FileDescriptorProto

from grpcreflect._gen.grpc.reflection.v1 import reflection_pb as v1
from grpcreflect.reflector import ReflectionSchema, Reflector

_SCHEMA = ReflectionSchema(
    response=v1.ServerReflectionResponse,
    file_descriptor_response=v1.FileDescriptorResponse,
    extension_number_response=v1.ExtensionNumberResponse,
    list_service_response=v1.ListServiceResponse,
    service_response=v1.ServiceResponse,
    error_response=v1.ErrorResponse,
)


def _request(**message_request_kwargs: object) -> v1.ServerReflectionRequest:
    message_request = (
        Oneof(**message_request_kwargs) if message_request_kwargs else None
    )
    return v1.ServerReflectionRequest(host="localhost", message_request=message_request)


async def _one_response(
    reflector: Reflector, request: v1.ServerReflectionRequest
) -> Oneof[str, object]:
    async def requests() -> AsyncIterator[v1.ServerReflectionRequest]:
        yield request

    responses = [resp async for resp in reflector.serve(requests(), ctx=None)]
    assert len(responses) == 1
    return responses[0].message_response


def _file_names(message_response: Oneof[str, object]) -> list[str]:
    assert message_response.field == "file_descriptor_response"
    return [
        FileDescriptorProto.from_binary(data).name
        for data in message_response.value.file_descriptor_proto
    ]


async def test_list_services_enumerates_registry_and_self() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    response = await _one_response(reflector, _request(field="list_services", value=""))
    assert response.field == "list_services_response"
    names = {service.name for service in response.value.service}
    assert names == {
        "grpc.reflection.v1.ServerReflection",
        "grpc.reflection.v1alpha.ServerReflection",
    }


async def test_file_by_filename_found() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    request = _request(
        field="file_by_filename", value="grpc/reflection/v1/reflection.proto"
    )
    response = await _one_response(reflector, request)
    assert _file_names(response) == ["grpc/reflection/v1/reflection.proto"]


async def test_file_by_filename_not_found() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    request = _request(field="file_by_filename", value="does/not/exist.proto")
    response = await _one_response(reflector, request)
    assert response.field == "error_response"
    assert response.value.error_code == 5
    assert response.value.error_message


async def test_file_containing_symbol_message() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    request = _request(
        field="file_containing_symbol",
        value="grpc.reflection.v1.ServerReflectionRequest",
    )
    response = await _one_response(reflector, request)
    assert _file_names(response) == ["grpc/reflection/v1/reflection.proto"]


async def test_file_containing_symbol_service() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    request = _request(
        field="file_containing_symbol", value="grpc.reflection.v1.ServerReflection"
    )
    response = await _one_response(reflector, request)
    assert _file_names(response) == ["grpc/reflection/v1/reflection.proto"]


async def test_file_containing_symbol_enum() -> None:
    reflector = Reflector(Registry(wkt.descriptor_pb.desc()), _SCHEMA)
    request = _request(field="file_containing_symbol", value="google.protobuf.Edition")
    response = await _one_response(reflector, request)
    assert _file_names(response) == ["google/protobuf/descriptor.proto"]


async def test_file_containing_symbol_not_found() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    request = _request(field="file_containing_symbol", value="does.not.Exist")
    response = await _one_response(reflector, request)
    assert response.field == "error_response"
    assert response.value.error_code == 5


async def test_file_containing_extension_found() -> None:
    registry = Registry(wkt.go_features_pb.desc())
    reflector = Reflector(registry, _SCHEMA)
    extension_request = v1.ExtensionRequest(
        containing_type="google.protobuf.FeatureSet", extension_number=1002
    )
    request = _request(field="file_containing_extension", value=extension_request)
    response = await _one_response(reflector, request)
    # The root file plus its dependency closure (descriptor.proto), in BFS order.
    assert _file_names(response) == [
        "google/protobuf/go_features.proto",
        "google/protobuf/descriptor.proto",
    ]


async def test_file_containing_extension_not_found() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    extension_request = v1.ExtensionRequest(
        containing_type="google.protobuf.FeatureSet", extension_number=9999
    )
    request = _request(field="file_containing_extension", value=extension_request)
    response = await _one_response(reflector, request)
    assert response.field == "error_response"
    assert response.value.error_code == 5


async def test_all_extension_numbers_of_type_with_extensions() -> None:
    registry = Registry(
        wkt.go_features_pb.desc(),
        wkt.java_features_pb.desc(),
        wkt.c_sharp_features_pb.desc(),
        wkt.cpp_features_pb.desc(),
    )
    reflector = Reflector(registry, _SCHEMA)
    request = _request(
        field="all_extension_numbers_of_type", value="google.protobuf.FeatureSet"
    )
    response = await _one_response(reflector, request)
    assert response.field == "all_extension_numbers_response"
    assert response.value.base_type_name == "google.protobuf.FeatureSet"
    assert response.value.extension_number == [1000, 1001, 1002, 1004]


async def test_all_extension_numbers_of_type_known_type_no_extensions() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    request = _request(
        field="all_extension_numbers_of_type",
        value="grpc.reflection.v1.ServerReflectionRequest",
    )
    response = await _one_response(reflector, request)
    assert response.field == "all_extension_numbers_response"
    assert response.value.extension_number == []


async def test_all_extension_numbers_of_type_unknown_type() -> None:
    reflector = Reflector(Registry(), _SCHEMA)
    request = _request(field="all_extension_numbers_of_type", value="does.not.Exist")
    response = await _one_response(reflector, request)
    assert response.field == "error_response"
    assert response.value.error_code == 5


async def test_invalid_message_request_raises_and_terminates_stream() -> None:
    reflector = Reflector(Registry(), _SCHEMA)

    async def requests() -> AsyncIterator[v1.ServerReflectionRequest]:
        yield _request()  # message_request unset
        pytest.fail("stream should have been aborted before a second request is pulled")

    with pytest.raises(ConnectError) as exc_info:
        async for _ in reflector.serve(requests(), ctx=None):
            pass
    assert exc_info.value.code == Code.INVALID_ARGUMENT


async def test_dependency_dedup_across_stream() -> None:
    """Reproduces Go's `fileDescriptorWithDependencies` dedup rules:

    - A dependency file already sent earlier in the stream is skipped on later requests.
    - The *root* file of a query is always sent, even if it was already sent as someone else's dependency earlier in the same stream.
    """
    registry = Registry(wkt.go_features_pb.desc(), wkt.java_features_pb.desc())
    reflector = Reflector(registry, _SCHEMA)

    go_extension = v1.ExtensionRequest(
        containing_type="google.protobuf.FeatureSet", extension_number=1002
    )
    java_extension = v1.ExtensionRequest(
        containing_type="google.protobuf.FeatureSet", extension_number=1001
    )

    async def requests() -> AsyncIterator[v1.ServerReflectionRequest]:
        yield _request(field="file_containing_extension", value=go_extension)
        yield _request(field="file_containing_extension", value=java_extension)
        yield _request(
            field="file_by_filename", value="google/protobuf/descriptor.proto"
        )

    responses = [resp async for resp in reflector.serve(requests(), ctx=None)]
    assert _file_names(responses[0].message_response) == [
        "google/protobuf/go_features.proto",
        "google/protobuf/descriptor.proto",
    ]
    # descriptor.proto was already sent as a dependency above, so it's
    # skipped here -- only the new root file is returned.
    assert _file_names(responses[1].message_response) == [
        "google/protobuf/java_features.proto",
    ]
    # Directly requesting descriptor.proto makes it the root of this query,
    # so it's sent again even though it's already in the per-stream set.
    assert _file_names(responses[2].message_response) == [
        "google/protobuf/descriptor.proto",
    ]
