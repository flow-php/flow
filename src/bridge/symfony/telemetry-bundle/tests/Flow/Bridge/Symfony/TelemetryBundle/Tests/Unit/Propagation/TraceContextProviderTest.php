<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Propagation;

use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(TraceContextProvider::class)]
final class TraceContextProviderTest extends TestCase
{
    private const string TRACE_ID = '0af7651916cd43dd8448eb211c80319c';

    private const string SPAN_ID = 'b7ad6b7169203331';

    public function test_current_exposes_the_active_traceparent(): void
    {
        $context = $this->providerWithActiveSpan()->current();

        static::assertArrayHasKey('traceparent', $context);
        static::assertStringStartsWith('00-' . self::TRACE_ID . '-' . self::SPAN_ID . '-', $context['traceparent']);
    }

    public function test_traceparent_returns_the_w3c_header_value(): void
    {
        static::assertStringStartsWith(
            '00-' . self::TRACE_ID . '-' . self::SPAN_ID . '-',
            $this->providerWithActiveSpan()->traceparent(),
        );
    }

    public function test_append_to_url_adds_context_to_a_plain_url(): void
    {
        $url = $this->providerWithActiveSpan()->appendToUrl('/next');

        static::assertStringStartsWith('/next?traceparent=00-' . self::TRACE_ID, $url);
    }

    public function test_append_to_url_uses_ampersand_when_a_query_already_exists(): void
    {
        $url = $this->providerWithActiveSpan()->appendToUrl('/next?page=2');

        static::assertStringContainsString('page=2&traceparent=00-' . self::TRACE_ID, $url);
    }

    public function test_returns_empty_results_without_an_active_span(): void
    {
        $provider = new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage());

        static::assertSame([], $provider->current());
        static::assertSame('', $provider->traceparent());
        static::assertSame('/next', $provider->appendToUrl('/next'));
    }

    private function providerWithActiveSpan(): TraceContextProvider
    {
        $storage = new MemoryContextStorage();
        $storage->attach((new Context())->withActiveSpan(SpanContext::create(
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex(self::SPAN_ID),
        )));

        return new TraceContextProvider(new W3CTraceContext(), $storage);
    }
}
