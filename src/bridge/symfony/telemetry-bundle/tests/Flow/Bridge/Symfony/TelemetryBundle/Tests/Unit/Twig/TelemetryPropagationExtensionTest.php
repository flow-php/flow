<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Propagation\TraceContextProvider;
use Flow\Bridge\Symfony\TelemetryBundle\Twig\TelemetryPropagationExtension;
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

#[CoversClass(TelemetryPropagationExtension::class)]
final class TelemetryPropagationExtensionTest extends TestCase
{
    private const string TRACE_ID = '0af7651916cd43dd8448eb211c80319c';

    private const string SPAN_ID = 'b7ad6b7169203331';

    public function test_meta_renders_a_tag_per_field(): void
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

        $extension = new TelemetryPropagationExtension(
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), $requestStack),
        );

        static::assertStringContainsString(
            '<meta name="traceparent" content="00-' . self::TRACE_ID . '-',
            $extension->renderMeta(),
        );
    }

    public function test_meta_is_empty_without_a_request_span(): void
    {
        $extension = new TelemetryPropagationExtension(
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), new RequestStack()),
        );

        static::assertSame('', $extension->renderMeta());
    }

    public function test_registers_the_propagation_functions(): void
    {
        $extension = new TelemetryPropagationExtension(
            new TraceContextProvider(new W3CTraceContext(), new MemoryContextStorage(), new RequestStack()),
        );

        $names = [];

        foreach ($extension->getFunctions() as $function) {
            $names[] = $function->getName();
        }

        static::assertSame(
            ['flow_traceparent', 'flow_trace_context', 'flow_trace_context_meta', 'flow_trace_context_url'],
            $names,
        );
    }
}
