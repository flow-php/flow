<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\ControllerSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Controller\TestController;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Event\ControllerArgumentsEvent;
use Symfony\Component\HttpKernel\Event\ExceptionEvent;
use Symfony\Component\HttpKernel\Event\ResponseEvent;
use Symfony\Component\HttpKernel\Event\ViewEvent;
use Symfony\Component\HttpKernel\HttpKernelInterface;
use Symfony\Component\HttpKernel\KernelEvents;

#[CoversClass(ControllerSpanSubscriber::class)]
final class ControllerSpanSubscriberTest extends TestCase
{
    public function test_subscribes_to_expected_events_with_priorities(): void
    {
        $events = ControllerSpanSubscriber::getSubscribedEvents();

        static::assertSame(['onControllerArguments', -10000], $events[KernelEvents::CONTROLLER_ARGUMENTS]);
        static::assertSame(['onComplete', 10000], $events[KernelEvents::VIEW]);
        static::assertSame(['onComplete', 10000], $events[KernelEvents::RESPONSE]);
        static::assertSame(['onException', 10000], $events[KernelEvents::EXCEPTION]);
    }

    public function test_opens_and_completes_body_span_on_response(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);
        $request->attributes->set('_route', 'test_index');

        $subscriber = new ControllerSpanSubscriber($telemetry);
        $kernel = $this->createStub(HttpKernelInterface::class);

        $subscriber->onControllerArguments(
            new ControllerArgumentsEvent(
                $kernel,
                [new TestController(), 'index'],
                [],
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );
        $subscriber->onComplete(
            new ResponseEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, new Response()),
        );

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame(TestController::class . '::index', $span->name());
        static::assertSame(SpanKind::INTERNAL, $span->kind());

        $attributes = $span->attributes();
        static::assertSame(TestController::class, $attributes['code.namespace']);
        static::assertSame('index', $attributes['code.function']);
        static::assertSame(TestController::class . '::index', $attributes['controller']);
        static::assertSame('test_index', $attributes['http.route']);

        static::assertSame($requestSpan->context()->spanId->toHex(), $span->context()->parentSpanId?->toHex());
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($span->status());
    }

    public function test_completes_body_span_on_view(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);

        $subscriber = new ControllerSpanSubscriber($telemetry);
        $kernel = $this->createStub(HttpKernelInterface::class);

        $subscriber->onControllerArguments(
            new ControllerArgumentsEvent(
                $kernel,
                [new TestController(), 'index'],
                [],
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );
        $subscriber->onComplete(new ViewEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, ['data' => 1]));

        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_completes_once_when_view_then_response(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);

        $subscriber = new ControllerSpanSubscriber($telemetry);
        $kernel = $this->createStub(HttpKernelInterface::class);

        $subscriber->onControllerArguments(
            new ControllerArgumentsEvent(
                $kernel,
                [new TestController(), 'index'],
                [],
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );
        $subscriber->onComplete(new ViewEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, ['data' => 1]));
        $subscriber->onComplete(
            new ResponseEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, new Response()),
        );

        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_records_exception_and_completes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);

        $subscriber = new ControllerSpanSubscriber($telemetry);
        $kernel = $this->createStub(HttpKernelInterface::class);

        $subscriber->onControllerArguments(
            new ControllerArgumentsEvent(
                $kernel,
                [new TestController(), 'exception'],
                [],
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );
        $subscriber->onException(
            new ExceptionEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, new RuntimeException('boom')),
        );
        $subscriber->onComplete(
            new ResponseEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, new Response()),
        );

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertTrue($spans[0]->status()?->isError());
        static::assertNotEmpty($spans[0]->events());
    }

    public function test_no_span_when_trace_controller_disabled(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);
        $requestSpan = $telemetry->tracer('flow.symfony.http_kernel', PackageVersion::get('symfony/http-kernel'))->span(
            'GET /test',
            SpanKind::SERVER,
        );

        $request = new Request();
        $request->attributes->set(HttpKernelSpanSubscriber::SPAN_ATTRIBUTE, $requestSpan);

        $subscriber = new ControllerSpanSubscriber($telemetry, traceController: false);
        $kernel = $this->createStub(HttpKernelInterface::class);

        $subscriber->onControllerArguments(
            new ControllerArgumentsEvent(
                $kernel,
                [new TestController(), 'index'],
                [],
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );
        $subscriber->onComplete(
            new ResponseEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, new Response()),
        );

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_no_span_when_request_span_absent(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $request = new Request();

        $subscriber = new ControllerSpanSubscriber($telemetry);
        $kernel = $this->createStub(HttpKernelInterface::class);

        $subscriber->onControllerArguments(
            new ControllerArgumentsEvent(
                $kernel,
                [new TestController(), 'index'],
                [],
                $request,
                HttpKernelInterface::MAIN_REQUEST,
            ),
        );
        $subscriber->onComplete(
            new ResponseEvent($kernel, $request, HttpKernelInterface::MAIN_REQUEST, new Response()),
        );

        static::assertCount(0, $spanProcessor->endedSpans());
    }
}
