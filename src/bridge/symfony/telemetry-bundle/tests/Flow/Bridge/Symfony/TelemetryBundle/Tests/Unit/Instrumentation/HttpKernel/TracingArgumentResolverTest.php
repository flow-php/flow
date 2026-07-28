<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingArgumentResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpKernel\Controller\ArgumentResolverInterface;

#[CoversClass(TracingArgumentResolver::class)]
final class TracingArgumentResolverTest extends TestCase
{
    public function test_traces_argument_resolution_when_request_span_present(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );
        $telemetry
            ->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))
            ->activate($requestSpan);
        $telemetry
            ->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))
            ->activate($requestSpan);

        $inner = $this->createStub(ArgumentResolverInterface::class);
        $inner->method('getArguments')->willReturn(['a', 'b']);

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);

        $arguments = (new TracingArgumentResolver($inner, $telemetry))->getArguments(
            $request,
            static fn(): null => null,
        );

        static::assertSame(['a', 'b'], $arguments);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('controller.get_arguments', $spans[0]->name());
        static::assertSame(SpanKind::INTERNAL, $spans[0]->kind());
        static::assertSame($requestSpan->context()->spanId->toHex(), $spans[0]->context()->parentSpanId?->toHex());
    }

    public function test_passes_through_when_request_span_absent(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $inner = $this->createStub(ArgumentResolverInterface::class);
        $inner->method('getArguments')->willReturn(['a']);

        $arguments = (new TracingArgumentResolver($inner, $telemetry))->getArguments(
            new Request(),
            static fn(): null => null,
        );

        static::assertSame(['a'], $arguments);
        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_records_exception_when_inner_resolver_fails(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );
        $telemetry
            ->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))
            ->activate($requestSpan);
        $telemetry
            ->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))
            ->activate($requestSpan);

        $inner = $this->createStub(ArgumentResolverInterface::class);
        $inner->method('getArguments')->willThrowException(new RuntimeException('arguments failed'));

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);

        $caught = false;

        try {
            (new TracingArgumentResolver($inner, $telemetry))->getArguments($request, static fn(): null => null);
        } catch (RuntimeException) {
            $caught = true;
        }

        static::assertTrue($caught);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertTrue($spans[0]->status()?->isError());
        static::assertSame(RuntimeException::class, $spans[0]->attributes()['error.type']);
    }
}
