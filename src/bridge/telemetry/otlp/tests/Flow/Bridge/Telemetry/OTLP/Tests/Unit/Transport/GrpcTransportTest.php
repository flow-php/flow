<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use Flow\Bridge\Telemetry\OTLP\Serializer\{GrpcSerializer, ProtobufSerializer};
use Flow\Bridge\Telemetry\OTLP\Transport\GrpcTransport;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Transport\TransportException;
use Google\Protobuf\Internal\Message;
use Grpc\BaseStub;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

final class GrpcTransportTest extends TestCase
{
    public function test_constructor_throws_when_grpc_extension_not_loaded() : void
    {
        if (\extension_loaded('grpc')) {
            self::markTestSkipped('This test requires grpc extension to NOT be loaded');
        }

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('grpc PHP extension is required');

        new GrpcTransport('localhost:4317', $this->createMock(GrpcSerializer::class));
    }

    #[RequiresPhpExtension('grpc')]
    public function test_creates_transport_with_custom_headers() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport(
            endpoint: 'localhost:4317',
            serializer: new ProtobufSerializer(),
            headers: ['Authorization' => 'Bearer token'],
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_creates_transport_with_default_options() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport('localhost:4317', new ProtobufSerializer());

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_creates_transport_with_secure_mode() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport(
            endpoint: 'localhost:4317',
            serializer: new ProtobufSerializer(),
            insecure: false,
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_send_after_shutdown_throws() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport('localhost:4317', new ProtobufSerializer());
        $transport->shutdown();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Cannot send after shutdown');

        $transport->send(Signals::traces([]));
    }

    #[RequiresPhpExtension('grpc')]
    public function test_shutdown_can_be_called_multiple_times() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport('localhost:4317', new ProtobufSerializer());

        $transport->shutdown();
        $transport->shutdown();

        self::addToAssertionCount(1);
    }

    private function skipIfGrpcDependenciesNotAvailable() : void
    {
        if (!\class_exists(BaseStub::class)) {
            self::markTestSkipped('The grpc/grpc package is not installed');
        }

        if (!\class_exists(Message::class)) {
            self::markTestSkipped('The google/protobuf package is not installed');
        }
    }
}
