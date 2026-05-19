<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\DSL;

use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Serializer\ProtobufSerializer;
use Flow\Bridge\Telemetry\OTLP\Tests\Context\Requirements;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\GrpcTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use PHPUnit\Framework\TestCase;

use function bin2hex;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_curl_transport;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_exporter;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_grpc_transport;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_json_serializer;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_protobuf_serializer;
use function Flow\Bridge\Telemetry\OTLP\DSL\otlp_stream_transport;
use function is_file;
use function random_bytes;
use function sys_get_temp_dir;
use function unlink;

final class FunctionsTest extends TestCase
{
    public function test_otlp_curl_transport_defaults_to_json_serializer(): void
    {
        static::assertInstanceOf(CurlTransport::class, otlp_curl_transport('http://localhost:4318'));
    }

    public function test_otlp_curl_transport_returns_curl_transport(): void
    {
        $transport = otlp_curl_transport('http://localhost:4318', otlp_json_serializer());

        static::assertInstanceOf(CurlTransport::class, $transport);
    }

    public function test_otlp_exporter_returns_otlp_exporter(): void
    {
        $transport = otlp_curl_transport('http://localhost:4318', otlp_json_serializer());

        static::assertInstanceOf(OTLPExporter::class, otlp_exporter($transport));
    }

    public function test_otlp_grpc_transport_returns_grpc_transport(): void
    {
        Requirements::requireGrpc();

        $transport = otlp_grpc_transport('localhost:4317');

        static::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_grpc_transport_with_headers(): void
    {
        Requirements::requireGrpc();

        $transport = otlp_grpc_transport(endpoint: 'localhost:4317', headers: ['Authorization' => 'Bearer token']);

        static::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_grpc_transport_with_secure_option(): void
    {
        Requirements::requireGrpc();

        $transport = otlp_grpc_transport(endpoint: 'localhost:4317', insecure: false);

        static::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_json_serializer_returns_json_serializer(): void
    {
        static::assertInstanceOf(JsonSerializer::class, otlp_json_serializer());
    }

    public function test_otlp_protobuf_serializer_returns_protobuf_serializer(): void
    {
        Requirements::requireProtobuf();

        static::assertInstanceOf(ProtobufSerializer::class, otlp_protobuf_serializer());
    }

    public function test_otlp_stream_transport_returns_stream_transport_for_file_path(): void
    {
        $path = sys_get_temp_dir() . '/flow-otlp-dsl-test-' . bin2hex(random_bytes(4)) . '.jsonl';

        try {
            static::assertInstanceOf(StreamTransport::class, otlp_stream_transport($path));
        } finally {
            if (is_file($path)) {
                unlink($path);
            }
        }
    }

    public function test_otlp_stream_transport_returns_stream_transport_for_php_uri(): void
    {
        static::assertInstanceOf(StreamTransport::class, otlp_stream_transport('php://stdout'));
        static::assertInstanceOf(StreamTransport::class, otlp_stream_transport('php://stderr'));
    }
}
