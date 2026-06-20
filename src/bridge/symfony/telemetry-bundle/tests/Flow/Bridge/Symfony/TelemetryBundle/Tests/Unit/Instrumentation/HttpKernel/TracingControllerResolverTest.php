<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingControllerResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ControllerResolverInterface;

#[CoversClass(TracingControllerResolver::class)]
final class TracingControllerResolverTest extends TestCase
{
    public function test_traces_resolution_when_request_span_present(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );

        $controller = static fn(): string => 'ok';
        $inner = $this->createStub(ControllerResolverInterface::class);
        $inner->method('getController')->willReturn($controller);

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);

        static::assertSame($controller, (new TracingControllerResolver($inner, $telemetry))->getController($request));

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('controller.get_callable', $spans[0]->name());
        static::assertSame(SpanKind::INTERNAL, $spans[0]->kind());
        static::assertSame($requestSpan->context()->spanId->toHex(), $spans[0]->context()->parentSpanId?->toHex());
    }

    public function test_passes_through_when_request_span_absent(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $controller = static fn(): string => 'ok';
        $inner = $this->createStub(ControllerResolverInterface::class);
        $inner->method('getController')->willReturn($controller);

        static::assertSame(
            $controller,
            (new TracingControllerResolver($inner, $telemetry))->getController(new Request()),
        );
        static::assertCount(0, $spanProcessor->endedSpans());
    }
}
