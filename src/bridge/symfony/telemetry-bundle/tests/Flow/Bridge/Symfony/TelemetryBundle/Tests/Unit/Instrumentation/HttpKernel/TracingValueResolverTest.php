<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingValueResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ValueResolverInterface;
use Symfony\Component\HttpKernel\ControllerMetadata\ArgumentMetadata;

use function iterator_to_array;

#[CoversClass(TracingValueResolver::class)]
final class TracingValueResolverTest extends TestCase
{
    public function test_traces_value_resolution_when_request_span_present(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );

        $inner = $this->createStub(ValueResolverInterface::class);
        $inner->method('resolve')->willReturn(['resolved']);

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);
        $argument = new ArgumentMetadata('id', 'int', false, false, null);

        $resolved = iterator_to_array((new TracingValueResolver($inner, $telemetry))->resolve($request, $argument));

        static::assertSame(['resolved'], $resolved);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('controller.argument_value_resolver', $spans[0]->name());
        static::assertSame(SpanKind::INTERNAL, $spans[0]->kind());
        static::assertSame($inner::class, $spans[0]->attributes()['code.namespace']);
        static::assertSame('id', $spans[0]->attributes()['controller.argument']);
        static::assertSame($requestSpan->context()->spanId->toHex(), $spans[0]->context()->parentSpanId?->toHex());
    }

    public function test_passes_through_when_request_span_absent(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $inner = $this->createStub(ValueResolverInterface::class);
        $inner->method('resolve')->willReturn(['resolved']);

        $resolved = iterator_to_array((new TracingValueResolver($inner, $telemetry))->resolve(
            new Request(),
            new ArgumentMetadata('id', 'int', false, false, null),
        ));

        static::assertSame(['resolved'], $resolved);
        static::assertCount(0, $spanProcessor->endedSpans());
    }
}
