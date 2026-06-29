<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Routing;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Routing\FakeUrlGenerator;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\RequestStack;
use Symfony\Component\Routing\RequestContext;

#[CoversClass(TraceContextUrlGenerator::class)]
final class TraceContextUrlGeneratorTest extends TestCase
{
    private const string TRACE_ID = '0af7651916cd43dd8448eb211c80319c';

    public function test_generate_appends_trace_context_to_the_generated_url(): void
    {
        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, SpanMother::create(
            'request',
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex('b7ad6b7169203331'),
            null,
            SpanKind::SERVER,
        ));
        $requestStack = new RequestStack();
        $requestStack->push($request);

        $generator = new TraceContextUrlGenerator(
            new FakeUrlGenerator('/checkout/step-2'),
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), $requestStack),
        );

        static::assertStringStartsWith(
            '/checkout/step-2?traceparent=00-' . self::TRACE_ID,
            $generator->generate('checkout_step_2'),
        );
    }

    public function test_generate_passes_through_when_there_is_no_request_span(): void
    {
        $generator = new TraceContextUrlGenerator(
            new FakeUrlGenerator('/checkout/step-2'),
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), new RequestStack()),
        );

        static::assertSame('/checkout/step-2', $generator->generate('checkout_step_2'));
    }

    public function test_delegates_request_context_to_the_inner_generator(): void
    {
        $inner = new FakeUrlGenerator();
        $generator = new TraceContextUrlGenerator(
            $inner,
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), new RequestStack()),
        );

        $context = new RequestContext('/app.php');
        $generator->setContext($context);

        static::assertSame($context, $generator->getContext());
        static::assertSame($context, $inner->getContext());
    }
}
