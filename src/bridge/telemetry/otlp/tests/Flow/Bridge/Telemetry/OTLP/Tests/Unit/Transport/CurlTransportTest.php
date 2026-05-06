<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Transport;

use function Flow\Bridge\Telemetry\OTLP\DSL\{otlp_curl_options, otlp_curl_transport, otlp_json_serializer};
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Transport\{CurlTransport, CurlTransportOptions};
use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{Span, SpanContext, SpanKind};
use Flow\Telemetry\Transport\TransportException;
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
                ->withTimeout(60)
                ->withConnectTimeout(30),
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
            ->withTimeout(60)
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
