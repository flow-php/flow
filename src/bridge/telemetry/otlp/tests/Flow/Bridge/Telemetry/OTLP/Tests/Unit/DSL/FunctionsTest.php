<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\DSL;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_transport, otlp_exporter, otlp_grpc_transport, otlp_json_serializer, otlp_protobuf_serializer};
use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Serializer\{JsonSerializer, ProtobufSerializer};
use Flow\Bridge\Telemetry\OTLP\Tests\Context\Requirements;
use Flow\Bridge\Telemetry\OTLP\Transport\{CurlTransport, GrpcTransport};
use PHPUnit\Framework\TestCase;

final class FunctionsTest extends TestCase
{
    public function test_otlp_curl_transport_returns_curl_transport() : void
    {
        $transport = otlp_curl_transport('http://localhost:4318', otlp_json_serializer());

        self::assertInstanceOf(CurlTransport::class, $transport);
    }

    public function test_otlp_exporter_returns_otlp_exporter() : void
    {
        $transport = otlp_curl_transport('http://localhost:4318', otlp_json_serializer());

        self::assertInstanceOf(OTLPExporter::class, otlp_exporter($transport));
    }

    public function test_otlp_grpc_transport_returns_grpc_transport() : void
    {
        Requirements::requireGrpc();

        $transport = otlp_grpc_transport('localhost:4317', otlp_protobuf_serializer());

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_grpc_transport_with_headers() : void
    {
        Requirements::requireGrpc();

        $transport = otlp_grpc_transport(
            endpoint: 'localhost:4317',
            serializer: otlp_protobuf_serializer(),
            headers: ['Authorization' => 'Bearer token'],
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_grpc_transport_with_secure_option() : void
    {
        Requirements::requireGrpc();

        $transport = otlp_grpc_transport(
            endpoint: 'localhost:4317',
            serializer: otlp_protobuf_serializer(),
            insecure: false,
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_json_serializer_returns_json_serializer() : void
    {
        self::assertInstanceOf(JsonSerializer::class, otlp_json_serializer());
    }

    public function test_otlp_protobuf_serializer_returns_protobuf_serializer() : void
    {
        Requirements::requireProtobuf();

        self::assertInstanceOf(ProtobufSerializer::class, otlp_protobuf_serializer());
    }
}
