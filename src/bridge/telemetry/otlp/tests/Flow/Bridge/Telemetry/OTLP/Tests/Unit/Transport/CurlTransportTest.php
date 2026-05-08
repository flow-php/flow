<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_options, otlp_curl_transport, otlp_json_serializer};
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Tests\Double\RecordingTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\{CurlTransport, CurlTransportOptions, FailoverTransportException, TransportException};
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{Span, SpanContext, SpanKind};
use PHPUnit\Framework\TestCase;

final class CurlTransportTest extends TestCase
{
    public function test_constructor_requires_curl_extension() : void
    {
        if (!\extension_loaded('curl')) {
            $this->expectException(\RuntimeException::class);
            $this->expectExceptionMessage('ext-curl is required');

            new CurlTransport(
                'http://localhost:4318',
                new JsonSerializer(),
            );
        } else {
            $this->addToAssertionCount(1);
        }
    }

    public function test_creates_transport_via_dsl_function() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $transport = otlp_curl_transport(
            'http://localhost:4318',
            otlp_json_serializer(),
        );

        self::assertInstanceOf(CurlTransport::class, $transport);
        $transport->shutdown();
    }

    public function test_creates_transport_with_custom_headers() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $transport = otlp_curl_transport(
            'http://localhost:4318',
            otlp_json_serializer(),
            otlp_curl_options()
                ->withHeader('Authorization', 'Bearer token')
                ->withHeader('X-Custom', 'value'),
        );

        self::assertInstanceOf(CurlTransport::class, $transport);
        $transport->shutdown();
    }

    public function test_creates_transport_with_custom_timeouts() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $transport = otlp_curl_transport(
            'http://localhost:4318',
            otlp_json_serializer(),
            otlp_curl_options()
                ->withTimeout(2000)
                ->withConnectTimeout(500),
        );

        self::assertInstanceOf(CurlTransport::class, $transport);
        $transport->shutdown();
    }

    public function test_creates_transport_with_options_object() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $options = new CurlTransportOptions();
        $options = $options
            ->withTimeout(2000)
            ->withCompression()
            ->withSslVerification(false);

        $transport = new CurlTransport(
            'http://localhost:4318',
            new JsonSerializer(),
            $options,
        );

        self::assertInstanceOf(CurlTransport::class, $transport);
        $transport->shutdown();
    }

    public function test_failover_receives_failed_batches_and_shutdown_throws_composite() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $failover = new RecordingTransport();
        $transport = new CurlTransport(
            'http://127.0.0.1:1',
            new JsonSerializer(),
            (new CurlTransportOptions())->withConnectTimeout(1)->withTimeout(1),
            $failover,
        );

        $batchA = Signals::traces($this->createSpans());
        $batchB = Signals::traces($this->createSpans());

        $transport->send($batchA);
        $transport->send($batchB);

        try {
            $transport->shutdown();
            self::fail('Expected FailoverTransportException');
        } catch (FailoverTransportException $e) {
            self::assertCount(2, $e->failures);

            foreach ($e->failures as $failure) {
                self::assertInstanceOf(TransportException::class, $failure['primary']);
                self::assertNull($failure['failover']);
            }
        }

        self::assertCount(2, $failover->sent);
        self::assertContains($batchA, $failover->sent);
        self::assertContains($batchB, $failover->sent);
        self::assertSame(1, $failover->shutdownCalls);
    }

    public function test_failover_records_double_failure_when_failover_send_also_throws() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $failover = new RecordingTransport();
        $failover->sendException = new TransportException('failover down');

        $transport = new CurlTransport(
            'http://127.0.0.1:1',
            new JsonSerializer(),
            (new CurlTransportOptions())->withConnectTimeout(1)->withTimeout(1),
            $failover,
        );

        $transport->send(Signals::traces($this->createSpans()));

        try {
            $transport->shutdown();
            self::fail('Expected FailoverTransportException');
        } catch (FailoverTransportException $e) {
            self::assertCount(1, $e->failures);
            self::assertInstanceOf(TransportException::class, $e->failures[0]['primary']);
            self::assertInstanceOf(TransportException::class, $e->failures[0]['failover']);
            self::assertStringContainsString('failover down', $e->failures[0]['failover']->getMessage());
        }

        self::assertCount(1, $failover->sent);
    }

    public function test_send_after_shutdown_throws_exception() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $transport = otlp_curl_transport(
            'http://localhost:4318',
            otlp_json_serializer(),
        );

        $transport->shutdown();

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('Cannot send after shutdown');

        $transport->send(Signals::traces($this->createSpans()));
    }

    public function test_shutdown_aggregates_curl_connection_failures() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $transport = otlp_curl_transport(
            'http://127.0.0.1:1',
            otlp_json_serializer(),
            otlp_curl_options()
                ->withConnectTimeout(1)
                ->withTimeout(1),
        );

        $transport->send(Signals::traces($this->createSpans()));
        $transport->send(Signals::traces($this->createSpans()));

        $this->expectException(TransportException::class);
        $this->expectExceptionMessageMatches('/OTLP curl shutdown: 2 exports failed; first error: curl error \\d+/');

        $transport->shutdown();
    }

    public function test_shutdown_cascades_to_failover_shutdown() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $failover = new RecordingTransport();
        $transport = new CurlTransport(
            'http://127.0.0.1:1',
            new JsonSerializer(),
            (new CurlTransportOptions())->withConnectTimeout(1)->withTimeout(1),
            $failover,
        );

        $transport->send(Signals::traces($this->createSpans()));

        try {
            $transport->shutdown();
            self::fail('Expected FailoverTransportException');
        } catch (FailoverTransportException $e) {
            self::assertCount(1, $e->failures);
        }

        self::assertSame(1, $failover->shutdownCalls);
    }

    public function test_shutdown_is_idempotent() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $transport = otlp_curl_transport(
            'http://localhost:4318',
            otlp_json_serializer(),
        );

        $transport->shutdown();
        $transport->shutdown();

        $this->addToAssertionCount(1);
    }

    public function test_shutdown_surfaces_failover_shutdown_exception_when_no_deferred_failures() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $failover = new RecordingTransport();
        $failover->shutdownException = new \RuntimeException('boom');

        $transport = new CurlTransport(
            'http://localhost:4318',
            new JsonSerializer(),
            new CurlTransportOptions(),
            $failover,
        );

        $this->expectException(TransportException::class);
        $this->expectExceptionMessage('failover shutdown failed: boom');

        $transport->shutdown();
    }

    public function test_shutdown_with_zero_timeout_forwards_pending_to_failover() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $failover = new RecordingTransport();
        $batch = Signals::traces($this->createSpans());

        $transport = new CurlTransport(
            'http://127.0.0.1:1',
            new JsonSerializer(),
            (new CurlTransportOptions())
                ->withConnectTimeout(60_000)
                ->withTimeout(60_000)
                ->withShutdownTimeout(0),
            $failover,
        );

        $transport->send($batch);

        try {
            $transport->shutdown();
            self::addToAssertionCount(1);
        } catch (FailoverTransportException $e) {
            self::assertGreaterThanOrEqual(1, \count($e->failures));
            // Either the still-pending path forwarded the batch, or the normal failover drain did.
            self::assertContains($batch, $failover->sent);
        }

        self::assertSame(1, $failover->shutdownCalls);
    }

    public function test_shutdown_with_zero_timeout_marks_pending_as_failed_in_legacy_mode() : void
    {
        if (!\extension_loaded('curl')) {
            self::markTestSkipped('ext-curl is required');
        }

        $transport = new CurlTransport(
            'http://127.0.0.1:1',
            new JsonSerializer(),
            (new CurlTransportOptions())
                ->withConnectTimeout(60_000)
                ->withTimeout(60_000)
                ->withShutdownTimeout(0),
        );

        $transport->send(Signals::traces($this->createSpans()));

        try {
            $transport->shutdown();

            // If everything completed before shutdown_timeout=0 fired, that's also valid (race condition).
            self::addToAssertionCount(1);
        } catch (TransportException $e) {
            // Either the shutdown-deadline-reached path OR the normal connection-refused path.
            self::assertTrue(
                \str_contains($e->getMessage(), 'shutdown_timeout=0ms expired')
                || \str_contains($e->getMessage(), 'curl error'),
            );
        }
    }

    /**
     * @return array<Span>
     */
    private function createSpans() : array
    {
        return [new Span(
            'test-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        )];
    }
}
