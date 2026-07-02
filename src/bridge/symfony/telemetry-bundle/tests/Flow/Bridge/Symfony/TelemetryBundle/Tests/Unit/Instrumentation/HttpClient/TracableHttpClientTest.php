<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpClient;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\ResponseStream;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\TracableHttpClient;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\TraceableResponse;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\FailingHttpClient;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\StreamingHttpClient;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\HttpClient\SuccessHttpClient;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;

#[CoversClass(TracableHttpClient::class)]
#[CoversClass(TraceableResponse::class)]
#[CoversClass(ResponseStream::class)]
final class TracableHttpClientTest extends TestCase
{
    public function test_emits_no_spans_when_tracing_is_suppressed(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::suppressed($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/users')->getContent();

        static::assertCount(
            0,
            $spanProcessor->endedSpans(),
            'HTTP client instrumentation must emit no spans while tracing is suppressed',
        );
    }

    public function test_request_defaults_host_to_unknown_when_missing(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', '/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('unknown', $spans[0]->attributes()['server.address']);
    }

    public function test_request_defaults_scheme_to_http_when_missing(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', '/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('http', $spans[0]->attributes()['url.scheme']);
    }

    public function test_request_does_not_complete_span_before_response_is_consumed(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $response = $tracable->request('GET', 'https://api.example.com/users');

        static::assertInstanceOf(TraceableResponse::class, $response);
        static::assertCount(0, $spanProcessor->endedSpans());

        $response->getContent();

        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_request_extracts_host_from_url(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('api.example.com', $spans[0]->attributes()['server.address']);
    }

    public function test_request_extracts_scheme_from_url(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('https', $spans[0]->attributes()['url.scheme']);
    }

    public function test_request_includes_client_name_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'my_api_client',
        );

        $tracable->request('GET', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('my_api_client', $spans[0]->attributes()['http.client.name']);
    }

    public function test_request_includes_http_status_code_attribute(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(200, $spans[0]->attributes()['http.response.status_code']);
    }

    public function test_request_includes_method_and_url_attributes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('POST', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('POST', $spans[0]->attributes()['http.request.method']);
        static::assertSame('https://api.example.com/users', $spans[0]->attributes()['url.full']);
    }

    public function test_request_records_exception_on_synchronous_failure(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new FailingHttpClient('Connection timeout'),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $exceptionThrown = false;

        try {
            $tracable->request('GET', 'https://api.example.com/users');
        } catch (RuntimeException) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $events = $spans[0]->events();
        static::assertCount(1, $events);
        static::assertSame('exception', $events[0]->name());

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Connection timeout', $status->description);
    }

    public function test_request_sets_error_status_for_4xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(404),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/missing')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('404', $spans[0]->attributes()['error.type']);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('HTTP 404', $status->description);
    }

    public function test_request_sets_error_status_for_5xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(500),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/error')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('500', $spans[0]->attributes()['error.type']);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('HTTP 500', $status->description);
    }

    public function test_request_leaves_status_unset_for_2xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(201),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('POST', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        // OTEL semconv: 1xx-3xx leaves the span status unset.
        static::assertNull($spans[0]->status());
    }

    public function test_request_leaves_status_unset_for_3xx_codes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(302),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/redirect')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        static::assertNull($spans[0]->status());
    }

    public function test_request_span_name_includes_method_and_host(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('POST', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('POST api.example.com', $spans[0]->name());
    }

    public function test_span_kind_is_client(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $tracable->request('GET', 'https://api.example.com/users')->getContent();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(SpanKind::CLIENT, $spans[0]->kind());
    }

    public function test_stream_completes_span_with_error_when_chunk_reports_error(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new StreamingHttpClient(200, 'Network is down'),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $response = $tracable->request('GET', 'https://api.example.com/users');

        foreach ($tracable->stream($response) as $chunk) {
            static::assertSame('Network is down', $chunk->getError());
        }

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Network is down', $status->description);
    }

    public function test_stream_unwraps_tracable_response_and_completes_span_on_last_chunk(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new StreamingHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $response = $tracable->request('GET', 'https://api.example.com/users');

        static::assertInstanceOf(TraceableResponse::class, $response);
        static::assertCount(0, $spanProcessor->endedSpans());

        $stream = $tracable->stream($response);
        static::assertInstanceOf(ResponseStream::class, $stream);

        $chunks = 0;

        foreach ($stream as $key => $_chunk) {
            static::assertSame($response, $key);
            $chunks++;
        }

        static::assertSame(2, $chunks);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(200, $spans[0]->attributes()['http.response.status_code']);
    }

    public function test_with_options_creates_new_instance(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $tracable = new TracableHttpClient(
            new SuccessHttpClient(200),
            TelemetryMother::withSpanProcessor($spanProcessor),
            'test.client',
        );

        $newTracable = $tracable->withOptions(['timeout' => 30]);

        static::assertNotSame($tracable, $newTracable);
        static::assertInstanceOf(TracableHttpClient::class, $newTracable);
    }
}
