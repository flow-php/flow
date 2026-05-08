<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use Flow\Bridge\Telemetry\OTLP\Tests\Double\RecordingTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\{FailoverTransportException, GrpcTransport, TransportException};
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Google\Protobuf\Internal\Message;
use Grpc\BaseStub;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;

final class GrpcTransportTest extends TestCase
{
    #[RequiresPhpExtension('grpc')]
    public function test_constructor_rejects_negative_shutdown_timeout() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Shutdown timeout must be non-negative');

        new GrpcTransport('localhost:4317', shutdownTimeoutMs: -1);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_constructor_rejects_negative_timeout() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Timeout must be non-negative');

        new GrpcTransport('localhost:4317', timeoutMs: -1);
    }

    public function test_constructor_throws_when_grpc_extension_not_loaded() : void
    {
        if (\extension_loaded('grpc')) {
            self::markTestSkipped('This test requires grpc extension to NOT be loaded');
        }

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('grpc PHP extension is required');

        new GrpcTransport('localhost:4317');
    }

    #[RequiresPhpExtension('grpc')]
    public function test_creates_transport_with_custom_headers() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport(
            endpoint: 'localhost:4317',
            headers: ['Authorization' => 'Bearer token'],
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_creates_transport_with_default_options() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport('localhost:4317');

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_creates_transport_with_secure_mode() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport(
            endpoint: 'localhost:4317',
            insecure: false,
        );

        self::assertInstanceOf(GrpcTransport::class, $transport);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_failover_receives_prior_batch_on_subsequent_send_and_throws_composite() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $failover = new RecordingTransport();
        $transport = new GrpcTransport(endpoint: '127.0.0.1:1', failover: $failover);

        $batchA = Signals::traces([SpanMother::withName('span-a')]);
        $batchB = Signals::traces([SpanMother::withName('span-b')]);

        $transport->send($batchA);

        try {
            $transport->send($batchB);
            self::fail('Expected FailoverTransportException for batch A');
        } catch (FailoverTransportException $e) {
            self::assertCount(1, $e->failures);
            self::assertNull($e->failures[0]['failover']);
        }

        try {
            $transport->shutdown();
            self::fail('Expected FailoverTransportException for batch B');
        } catch (FailoverTransportException $e) {
            self::assertCount(1, $e->failures);
            self::assertNull($e->failures[0]['failover']);
        }

        self::assertCount(2, $failover->sent);
        self::assertContains($batchA, $failover->sent);
        self::assertContains($batchB, $failover->sent);
        self::assertSame(1, $failover->shutdownCalls);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_failover_records_double_failure_when_failover_send_also_throws() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $failover = new RecordingTransport();
        $failover->sendException = new TransportException('failover down');

        $transport = new GrpcTransport(endpoint: '127.0.0.1:1', failover: $failover);

        $transport->send(Signals::traces([SpanMother::withName('span-a')]));

        try {
            $transport->shutdown();
            self::fail('Expected FailoverTransportException');
        } catch (FailoverTransportException $e) {
            self::assertCount(1, $e->failures);
            self::assertInstanceOf(\Throwable::class, $e->failures[0]['primary']);
            self::assertInstanceOf(TransportException::class, $e->failures[0]['failover']);
            self::assertStringContainsString('failover down', $e->failures[0]['failover']->getMessage());
        }

        self::assertCount(1, $failover->sent);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_send_after_shutdown_throws() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport('localhost:4317');
        $transport->shutdown();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Cannot send after shutdown');

        $transport->send(Signals::traces([]));
    }

    #[RequiresPhpExtension('grpc')]
    public function test_shutdown_aggregates_grpc_failures() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport(endpoint: '127.0.0.1:1');

        $transport->send(Signals::traces([SpanMother::withName('span-a')]));
        $transport->send(Signals::traces([SpanMother::withName('span-b')]));

        $this->expectException(TransportException::class);
        $this->expectExceptionMessageMatches('/OTLP gRPC shutdown: 2 exports failed; first error: gRPC status \\d+/');

        $transport->shutdown();
    }

    #[RequiresPhpExtension('grpc')]
    public function test_shutdown_can_be_called_multiple_times() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport('localhost:4317');

        $transport->shutdown();
        $transport->shutdown();

        self::addToAssertionCount(1);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_shutdown_cascades_to_failover_shutdown() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $failover = new RecordingTransport();
        $transport = new GrpcTransport(endpoint: '127.0.0.1:1', failover: $failover);

        $transport->send(Signals::traces([SpanMother::withName('span-a')]));

        try {
            $transport->shutdown();
            self::fail('Expected FailoverTransportException');
        } catch (FailoverTransportException $e) {
            self::assertCount(1, $e->failures);
        }

        self::assertSame(1, $failover->shutdownCalls);
    }

    #[RequiresPhpExtension('grpc')]
    public function test_shutdown_surfaces_failover_shutdown_exception_when_no_deferred_failures() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $failover = new RecordingTransport();
        $failover->shutdownException = new \RuntimeException('boom');

        $transport = new GrpcTransport(endpoint: 'localhost:4317', failover: $failover);

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('failover shutdown failed: boom');

        $transport->shutdown();
    }

    #[RequiresPhpExtension('grpc')]
    public function test_shutdown_with_zero_timeout_cancels_pending_calls_legacy_mode() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $transport = new GrpcTransport(
            endpoint: '127.0.0.1:1',
            timeoutMs: 60_000,
            shutdownTimeoutMs: 0,
        );

        $transport->send(Signals::traces([SpanMother::withName('span-a')]));

        try {
            $transport->shutdown();
            self::addToAssertionCount(1);
        } catch (TransportException $e) {
            self::assertTrue(
                \str_contains($e->getMessage(), 'shutdown_timeout=0ms expired')
                || \str_contains($e->getMessage(), 'gRPC status'),
            );
        }
    }

    #[RequiresPhpExtension('grpc')]
    public function test_shutdown_with_zero_timeout_forwards_pending_to_failover() : void
    {
        $this->skipIfGrpcDependenciesNotAvailable();

        $failover = new RecordingTransport();
        $batch = Signals::traces([SpanMother::withName('span-a')]);

        $transport = new GrpcTransport(
            endpoint: '127.0.0.1:1',
            timeoutMs: 60_000,
            shutdownTimeoutMs: 0,
            failover: $failover,
        );

        $transport->send($batch);

        try {
            $transport->shutdown();
            self::addToAssertionCount(1);
        } catch (FailoverTransportException $e) {
            self::assertGreaterThanOrEqual(1, \count($e->failures));
        }

        self::assertSame(1, $failover->shutdownCalls);
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
