<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\DSL;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_grpc_transport, otlp_http_transport, otlp_json_serializer, otlp_protobuf_serializer};
use Flow\Bridge\Telemetry\OTLP\Serializer\{JsonSerializer, ProtobufSerializer};
use Flow\Bridge\Telemetry\OTLP\Transport\{GrpcTransport, HttpTransport};
use Google\Protobuf\Internal\Message;
use Grpc\BaseStub;
use Opentelemetry\Proto\Collector\Trace\V1\TraceServiceClient;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestFactoryInterface, StreamFactoryInterface};

final class FunctionsTest extends TestCase
{
    public function test_otlp_grpc_transport_returns_grpc_transport() : void
    {
        $this->skipIfGrpcNotAvailable();

        $transport = otlp_grpc_transport('localhost:4317', otlp_protobuf_serializer());

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_grpc_transport_with_headers() : void
    {
        $this->skipIfGrpcNotAvailable();

        $transport = otlp_grpc_transport(
            endpoint: 'localhost:4317',
            serializer: otlp_protobuf_serializer(),
            headers: ['Authorization' => 'Bearer token'],
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_grpc_transport_with_secure_option() : void
    {
        $this->skipIfGrpcNotAvailable();

        $transport = otlp_grpc_transport(
            endpoint: 'localhost:4317',
            serializer: otlp_protobuf_serializer(),
            insecure: false,
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    public function test_otlp_http_transport_returns_http_transport() : void
    {
        $client = $this->createMock(ClientInterface::class);
        $requestFactory = $this->createMock(RequestFactoryInterface::class);
        $streamFactory = $this->createMock(StreamFactoryInterface::class);

        $transport = otlp_http_transport($client, $requestFactory, $streamFactory, 'http://localhost:4318', otlp_json_serializer());

        self::assertInstanceOf(HttpTransport::class, $transport);
    }

    public function test_otlp_json_serializer_returns_json_serializer() : void
    {
        $serializer = otlp_json_serializer();

        self::assertInstanceOf(JsonSerializer::class, $serializer);
    }

    public function test_otlp_protobuf_serializer_returns_protobuf_serializer() : void
    {
        $this->skipIfProtobufNotAvailable();

        $serializer = otlp_protobuf_serializer();

        self::assertInstanceOf(ProtobufSerializer::class, $serializer);
    }

    private function skipIfGrpcNotAvailable() : void
    {
        if (!\extension_loaded('grpc')) {
            self::markTestSkipped('The grpc extension is not available');
        }

        if (!\class_exists(BaseStub::class)) {
            self::markTestSkipped('The grpc/grpc package is not installed');
        }

        if (!\class_exists(Message::class)) {
            self::markTestSkipped('The google/protobuf package is not installed');
        }

        if (!\class_exists(TraceServiceClient::class)) {
            self::markTestSkipped('The open-telemetry/gen-otlp-protobuf package is not installed');
        }
    }

    private function skipIfProtobufNotAvailable() : void
    {
        if (!\class_exists(Message::class)) {
            self::markTestSkipped('The google/protobuf package is not installed');
        }

        if (!\class_exists(TraceServiceClient::class)) {
            self::markTestSkipped('The open-telemetry/gen-otlp-protobuf package is not installed');
        }
    }
}
