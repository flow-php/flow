<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Routing;

use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Routing\FakeUrlGenerator;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Routing\RequestContext;

#[CoversClass(TraceContextUrlGenerator::class)]
final class TraceContextUrlGeneratorTest extends TestCase
{
    private const string TRACE_ID = '0af7651916cd43dd8448eb211c80319c';

    public function test_generate_appends_trace_context_to_the_generated_url(): void
    {
        $generator = new TraceContextUrlGenerator(
            new FakeUrlGenerator('/checkout/step-2'),
            $this->providerWithActiveSpan(),
        );

        $url = $generator->generate('checkout_step_2');

        static::assertStringStartsWith('/checkout/step-2?traceparent=00-' . self::TRACE_ID, $url);
    }

    public function test_generate_passes_through_when_there_is_no_active_span(): void
    {
        $generator = new TraceContextUrlGenerator(
            new FakeUrlGenerator('/checkout/step-2'),
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage()),
        );

        static::assertSame('/checkout/step-2', $generator->generate('checkout_step_2'));
    }

    public function test_delegates_request_context_to_the_inner_generator(): void
    {
        $inner = new FakeUrlGenerator();
        $generator = new TraceContextUrlGenerator($inner, $this->providerWithActiveSpan());

        $context = new RequestContext('/app.php');
        $generator->setContext($context);

        static::assertSame($context, $generator->getContext());
        static::assertSame($context, $inner->getContext());
    }

    private function providerWithActiveSpan(): TraceContextProvider
    {
        $storage = new MemoryContextStorage();
        $storage->attach((new Context())->withActiveSpan(SpanContext::create(
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex('b7ad6b7169203331'),
        )));

        return new TraceContextProvider(new W3CTraceContext(), $storage);
    }
}
