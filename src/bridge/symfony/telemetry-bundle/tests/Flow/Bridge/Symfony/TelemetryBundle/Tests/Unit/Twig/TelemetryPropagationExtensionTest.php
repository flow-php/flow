<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Twig\TelemetryPropagationExtension;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\W3CTraceContext;
use Flow\Telemetry\Tracer\SpanContext;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(TelemetryPropagationExtension::class)]
final class TelemetryPropagationExtensionTest extends TestCase
{
    private const string TRACE_ID = '0af7651916cd43dd8448eb211c80319c';

    private const string SPAN_ID = 'b7ad6b7169203331';

    public function test_meta_renders_a_tag_per_field(): void
    {
        $meta = $this->extensionWithActiveSpan()->renderMeta();

        static::assertStringContainsString('<meta name="traceparent" content="00-' . self::TRACE_ID . '-', $meta);
    }

    public function test_meta_is_empty_without_an_active_span(): void
    {
        $extension = new TelemetryPropagationExtension(
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage()),
        );

        static::assertSame('', $extension->renderMeta());
    }

    public function test_registers_the_propagation_functions(): void
    {
        $names = [];

        foreach ($this->extensionWithActiveSpan()->getFunctions() as $function) {
            $names[] = $function->getName();
        }

        static::assertSame(
            ['flow_traceparent', 'flow_trace_context', 'flow_trace_context_meta', 'flow_trace_context_url'],
            $names,
        );
    }

    private function extensionWithActiveSpan(): TelemetryPropagationExtension
    {
        $storage = new MemoryContextStorage();
        $storage->attach((new Context())->withActiveSpan(SpanContext::create(
            TraceId::fromHex(self::TRACE_ID),
            SpanId::fromHex(self::SPAN_ID),
        )));

        return new TelemetryPropagationExtension(new TraceContextProvider(new W3CTraceContext(), $storage));
    }
}
