<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Propagation;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\RequestStack;

#[CoversClass(TraceContextProvider::class)]
final class TraceContextProviderTest extends TestCase
{
    private const string TRACE_ID = '0af7651916cd43dd8448eb211c80319c';

    private const string SPAN_ID = 'b7ad6b7169203331';

    public function test_current_emits_the_request_span_traceparent(): void
    {
        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, SpanMother::create(
            'request',
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex(self::SPAN_ID),
            null,
            SpanKind::SERVER,
        ));
        $requestStack = new RequestStack();
        $requestStack->push($request);

        $context = (new TraceContextProvider(
            new W3CTraceContext(),
            new MemoryContextStorage(),
            $requestStack,
        ))->current();

        static::assertArrayHasKey('traceparent', $context);
        static::assertStringStartsWith('00-' . self::TRACE_ID . '-' . self::SPAN_ID . '-', $context['traceparent']);
    }

    public function test_traceparent_returns_the_request_span_w3c_value(): void
    {
        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, SpanMother::create(
            'request',
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex(self::SPAN_ID),
            null,
            SpanKind::SERVER,
        ));
        $requestStack = new RequestStack();
        $requestStack->push($request);

        static::assertStringStartsWith(
            '00-' . self::TRACE_ID . '-' . self::SPAN_ID . '-',
            (new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), $requestStack))->traceparent(),
        );
    }

    public function test_request_span_wins_over_the_active_inner_span(): void
    {
        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, SpanMother::create(
            'request',
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex(self::SPAN_ID),
            null,
            SpanKind::SERVER,
        ));
        $requestStack = new RequestStack();
        $requestStack->push($request);

        $storage = new MemoryContextStorage();
        $storage->attach((new Context())->withActiveSpan(SpanContext::create(
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex('aaaaaaaaaaaaaaaa'),
        )));

        $context = (new TraceContextProvider(new W3CTraceContext(), $storage, $requestStack))->current();

        static::assertStringStartsWith('00-' . self::TRACE_ID . '-' . self::SPAN_ID . '-', $context['traceparent']);
        static::assertStringNotContainsString('aaaaaaaaaaaaaaaa', $context['traceparent']);
    }

    public function test_append_to_url_adds_request_context_to_a_plain_url(): void
    {
        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, SpanMother::create(
            'request',
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex(self::SPAN_ID),
            null,
            SpanKind::SERVER,
        ));
        $requestStack = new RequestStack();
        $requestStack->push($request);

        $url = (new TraceContextProvider(
            new W3CTraceContext(),
            new MemoryContextStorage(),
            $requestStack,
        ))->appendToUrl('/next');

        static::assertStringStartsWith('/next?traceparent=00-' . self::TRACE_ID, $url);
    }

    public function test_append_to_url_uses_ampersand_when_a_query_already_exists(): void
    {
        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, SpanMother::create(
            'request',
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex(self::SPAN_ID),
            null,
            SpanKind::SERVER,
        ));
        $requestStack = new RequestStack();
        $requestStack->push($request);

        $url = (new TraceContextProvider(
            new W3CTraceContext(),
            new MemoryContextStorage(),
            $requestStack,
        ))->appendToUrl('/next?page=2');

        static::assertStringContainsString('page=2&traceparent=00-' . self::TRACE_ID, $url);
    }

    public function test_returns_empty_when_there_is_no_main_request(): void
    {
        $provider = new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), new RequestStack());

        static::assertSame([], $provider->current());
        static::assertSame('', $provider->traceparent());
        static::assertSame('/next', $provider->appendToUrl('/next'));
    }

    public function test_returns_empty_when_the_main_request_has_no_span(): void
    {
        $requestStack = new RequestStack();
        $requestStack->push(new Request());

        $provider = new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), $requestStack);

        static::assertSame([], $provider->current());
    }
}
