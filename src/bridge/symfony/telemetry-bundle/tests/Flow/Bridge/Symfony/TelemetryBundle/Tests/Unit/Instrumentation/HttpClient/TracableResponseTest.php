<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpClient;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\TraceableResponse;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\MockResponse;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\SpyResponse;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\SuccessHttpClient;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use TypeError;

#[CoversClass(TraceableResponse::class)]
final class TracableResponseTest extends TestCase
{
    public function test_cancel_completes_span_and_records_status_without_calling_get_status_code(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);
        $spy = new SpyResponse(200);

        $response = new TraceableResponse($tracer, $spy, $span);
        $response->cancel();

        static::assertTrue($spy->cancelCalled);
        static::assertFalse($spy->getStatusCodeCalled);
        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());
        static::assertSame(200, $span->attributes()['http.response.status_code']);
    }

    public function test_construction_does_not_touch_inner_response(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);
        $spy = new SpyResponse(200);

        $response = new TraceableResponse($tracer, $spy, $span);

        static::assertFalse($spy->anyMethodCalled());
        static::assertFalse($span->isEnded());
        static::assertCount(0, $processor->endedSpans());
        static::assertInstanceOf(TraceableResponse::class, $response);
    }

    public function test_destruct_completes_span_as_backstop(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $spy = new SpyResponse(200);
        $response = new TraceableResponse($tracer, $spy, $span);
        unset($response);

        static::assertTrue($spy->destructCalled);
        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());
        static::assertSame(200, $span->attributes()['http.response.status_code']);
    }

    public function test_get_content_completes_span_with_error_status_for_4xx(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(404), $span);
        $response->getContent();

        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('HTTP 404', $status->description);
    }

    public function test_get_content_completes_span_with_ok_status(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200), $span);
        $response->getContent();

        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());
        static::assertSame(200, $span->attributes()['http.response.status_code']);

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_get_headers_records_status_without_completing_span(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200), $span);
        $response->getHeaders();

        static::assertFalse($span->isEnded());
        static::assertCount(0, $processor->endedSpans());
        static::assertSame(200, $span->attributes()['http.response.status_code']);
    }

    public function test_get_info_delegates_without_span_effect(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200), $span);

        static::assertSame(200, $response->getInfo('http_code'));
        static::assertFalse($span->isEnded());
        static::assertCount(0, $processor->endedSpans());
        static::assertArrayNotHasKey('http.response.status_code', $span->attributes());
    }

    public function test_get_headers_throwable_records_exception_and_completes(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200, new RuntimeException('boom')), $span);

        $exceptionThrown = false;

        try {
            $response->getHeaders();
        } catch (RuntimeException) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown);
        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());
        static::assertCount(1, $span->events());

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('boom', $status->description);
    }

    public function test_get_status_code_records_status_without_completing_span(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200), $span);

        static::assertSame(200, $response->getStatusCode());
        static::assertFalse($span->isEnded());
        static::assertCount(0, $processor->endedSpans());
        static::assertSame(200, $span->attributes()['http.response.status_code']);
    }

    public function test_get_status_code_throwable_records_exception_and_completes(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200, new RuntimeException('boom')), $span);

        $exceptionThrown = false;

        try {
            $response->getStatusCode();
        } catch (RuntimeException) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown);
        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());
        static::assertCount(1, $span->events());

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('boom', $status->description);
    }

    public function test_span_is_completed_exactly_once(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200), $span);
        $response->getContent();
        $response->getContent();
        unset($response);

        static::assertCount(1, $processor->endedSpans());
    }

    public function test_throwable_during_access_records_exception_and_completes(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200, new RuntimeException('boom')), $span);

        $exceptionThrown = false;

        try {
            $response->getContent();
        } catch (RuntimeException) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown);
        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());

        $events = $span->events();
        static::assertCount(1, $events);
        static::assertSame('exception', $events[0]->name());

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('boom', $status->description);
    }

    public function test_stream_rejects_non_tracable_response(): void
    {
        $this->expectException(TypeError::class);

        foreach (TraceableResponse::stream(new SuccessHttpClient(200), [new MockResponse(200)], null) as $_chunk) {
            // iteration triggers the generator, which rejects the non-TracableResponse item
        }
    }

    public function test_to_array_completes_span(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200), $span);
        $response->toArray();

        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());
        static::assertSame(200, $span->attributes()['http.response.status_code']);
    }

    public function test_to_array_throwable_records_exception_and_completes(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $tracer = TelemetryMother::withSpanProcessor($processor)->tracer('test');
        $span = $tracer->span('GET host', SpanKind::CLIENT);

        $response = new TraceableResponse($tracer, new SpyResponse(200, new RuntimeException('boom')), $span);

        $exceptionThrown = false;

        try {
            $response->toArray();
        } catch (RuntimeException) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown);
        static::assertTrue($span->isEnded());
        static::assertCount(1, $processor->endedSpans());
        static::assertCount(1, $span->events());

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('boom', $status->description);
    }
}
