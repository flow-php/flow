<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr18\Telemetry\Tests\Unit;

use Flow\Bridge\Psr18\Telemetry\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Tracer\SpanKind;
use Nyholm\Psr7\Request;
use Nyholm\Psr7\Response;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientInterface;
use RuntimeException;

use function Flow\Bridge\Psr18\Telemetry\DSL\psr18_traceable_client;
use function Flow\Telemetry\DSL\url_sanitizer;

final class PSR18TraceableClientTest extends TestCase
{
    public function test_custom_sanitizer_redacts_configured_query_parameters_and_userinfo(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());

        $client = $this->createStub(ClientInterface::class);
        $client->method('sendRequest')->willReturn(new Response(200));

        psr18_traceable_client(
            $client,
            TelemetryMother::withSpanProcessor($spanProcessor),
            url_sanitizer(['access_token']),
        )->sendRequest(new Request('GET', 'https://u:p@graph.facebook.com/me?access_token=SECRET&fields=id'));

        static::assertSame(
            'https://REDACTED:REDACTED@graph.facebook.com/me?access_token=REDACTED&fields=id',
            $spanProcessor->endedSpans()[0]->attributes()['url.full'],
        );
    }

    public function test_default_sanitizer_redacts_sensitive_query_parameters(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());

        $client = $this->createStub(ClientInterface::class);
        $client->method('sendRequest')->willReturn(new Response(200));

        psr18_traceable_client($client, TelemetryMother::withSpanProcessor($spanProcessor))->sendRequest(new Request(
            'GET',
            'https://bucket.s3.amazonaws.com/file?X-Amz-Signature=SECRET&access_token=SECRET&fields=id',
        ));

        static::assertSame(
            'https://bucket.s3.amazonaws.com/file?X-Amz-Signature=REDACTED&access_token=REDACTED&fields=id',
            $spanProcessor->endedSpans()[0]->attributes()['url.full'],
        );
    }

    public function test_emits_no_spans_when_tracing_is_suppressed(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = TelemetryMother::suppressed($spanProcessor);

        $client = $this->createStub(ClientInterface::class);
        $client->method('sendRequest')->willReturn(new Response(200));

        psr18_traceable_client($client, $telemetry)->sendRequest(new Request('GET', 'https://api.example.com/users'));

        static::assertCount(
            0,
            $spanProcessor->endedSpans(),
            'PSR-18 client instrumentation must emit no spans while tracing is suppressed',
        );
    }

    public function test_exception_message_and_status_description_hold_redacted_url(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());

        $client = $this->createStub(ClientInterface::class);
        $client
            ->method('sendRequest')
            ->willThrowException(
                new RuntimeException('Could not reach "https://graph.facebook.com/me?access_token=SECRET".'),
            );

        $this->expectException(RuntimeException::class);

        try {
            psr18_traceable_client(
                $client,
                TelemetryMother::withSpanProcessor($spanProcessor),
                url_sanitizer(['access_token']),
            )->sendRequest(new Request('GET', 'https://graph.facebook.com/me?access_token=SECRET'));
        } finally {
            $span = $spanProcessor->endedSpans()[0];
            static::assertSame(
                'Could not reach "https://graph.facebook.com/me?access_token=REDACTED".',
                $span->events()[0]->attributes()['exception.message'],
            );
            static::assertSame(
                'Could not reach "https://graph.facebook.com/me?access_token=REDACTED".',
                $span->status()?->description,
            );
        }
    }

    public function test_exception_is_recorded_and_rethrown(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $exception = new RuntimeException('Connection failed');
        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willThrowException($exception);

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Connection failed');

        try {
            $traceableClient->sendRequest($request);
        } finally {
            $spans = $spanProcessor->endedSpans();
            static::assertCount(1, $spans);
            $span = $spans[0];

            $events = $span->events();
            static::assertCount(1, $events);
            static::assertSame('exception', $events[0]->name());

            $eventAttributes = $events[0]->attributes();
            static::assertSame(RuntimeException::class, $eventAttributes['exception.type']);
            static::assertSame('Connection failed', $eventAttributes['exception.message']);

            $status = $span->status();

            if ($status === null) {
                static::fail('Expected span to have status');
            }

            static::assertTrue($status->isError());
            static::assertSame('Connection failed', $status->description);
            static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
        }
    }

    public function test_request_with_4xx_status_creates_error_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(404));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users/123');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        $status = $span->status();

        if ($status === null) {
            static::fail('Expected span to have status');
        }

        static::assertTrue($status->isError());
        static::assertSame('HTTP 404', $status->description);
        static::assertSame(404, $span->attributes()['http.response.status_code']);
        static::assertSame('404', $span->attributes()['error.type']);
    }

    public function test_request_with_5xx_status_creates_error_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(500));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        $status = $span->status();

        if ($status === null) {
            static::fail('Expected span to have status');
        }

        static::assertTrue($status->isError());
        static::assertSame('HTTP 500', $status->description);
        static::assertSame(500, $span->attributes()['http.response.status_code']);
        static::assertSame('500', $span->attributes()['error.type']);
    }

    public function test_span_has_correct_attributes(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('POST', 'https://api.example.com:8080/users?page=1');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        $attributes = $span->attributes();
        static::assertSame('POST', $attributes['http.request.method']);
        static::assertSame('https://api.example.com:8080/users?page=1', $attributes['url.full']);
        static::assertSame('https', $attributes['url.scheme']);
        static::assertSame('api.example.com', $attributes['server.address']);
        static::assertSame(8080, $attributes['server.port']);
        static::assertSame(200, $attributes['http.response.status_code']);
    }

    public function test_span_kind_is_client(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $traceableClient->sendRequest($request);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(SpanKind::CLIENT, $spans[0]->kind());
    }

    public function test_successful_request_creates_span_with_ok_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $telemetry = TelemetryMother::withSpanProcessor($spanProcessor);

        $mockClient = $this->createStub(ClientInterface::class);
        $mockClient->method('sendRequest')->willReturn(new Response(200));

        $traceableClient = psr18_traceable_client($mockClient, $telemetry);

        $request = new Request('GET', 'https://api.example.com/users');
        $response = $traceableClient->sendRequest($request);

        static::assertSame(200, $response->getStatusCode());

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $span = $spans[0];

        static::assertSame('GET', $span->name());
        // OTEL HTTP semconv: server.port is Required on client spans, even for scheme defaults.
        static::assertSame(443, $span->attributes()['server.port']);

        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($span->status());
        static::assertArrayNotHasKey('error.type', $span->attributes());
    }
}
