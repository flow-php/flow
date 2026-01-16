<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Tracer;

use function Flow\Telemetry\DSL\context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus, TracerProvider};
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

final class TracingIntegrationTest extends TestCase
{
    private Resource $resource;

    protected function setUp() : void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_complete_tracing_workflow() : void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());

        $tracer = $provider->tracer($this->resource, 'my-service', '1.0.0');

        $rootSpan = $tracer->span('handle-request', SpanKind::SERVER);
        $rootSpan->setAttribute('http.method', 'POST');
        $rootSpan->setAttribute('http.url', '/api/orders');

        $dbSpan = $tracer->span('database-query', SpanKind::CLIENT);
        $dbSpan->setAttribute('db.system', 'postgresql');
        $dbSpan->setAttribute('db.statement', 'SELECT * FROM orders');
        $dbSpan->setStatus(SpanStatus::ok());
        $tracer->complete($dbSpan);

        $rootSpan->setStatus(SpanStatus::ok());
        $tracer->complete($rootSpan);

        self::assertCount(2, $processor->endedSpans());

        $endedDbSpan = $processor->endedSpans()[0];
        self::assertSame('database-query', $endedDbSpan->name());
        self::assertSame(SpanKind::CLIENT, $endedDbSpan->kind());
        self::assertNotNull($endedDbSpan->context()->parentSpanId);
        self::assertTrue($endedDbSpan->context()->parentSpanId->equals($rootSpan->context()->spanId));

        $endedRootSpan = $processor->endedSpans()[1];
        self::assertSame('handle-request', $endedRootSpan->name());
        self::assertSame(SpanKind::SERVER, $endedRootSpan->kind());
        self::assertTrue($endedRootSpan->context()->isRoot());
    }

    public function test_deeply_nested_spans() : void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());
        $tracer = $provider->tracer($this->resource, 'test');

        $level1 = $tracer->span('level-1');
        $level2 = $tracer->span('level-2');
        $level3 = $tracer->span('level-3');
        $level4 = $tracer->span('level-4');

        $tracer->complete($level4);
        $tracer->complete($level3);
        $tracer->complete($level2);
        $tracer->complete($level1);

        self::assertCount(4, $processor->endedSpans());

        self::assertNotNull($level4->context()->parentSpanId);
        self::assertNotNull($level3->context()->parentSpanId);
        self::assertNotNull($level2->context()->parentSpanId);
        self::assertTrue($level4->context()->parentSpanId->equals($level3->context()->spanId));
        self::assertTrue($level3->context()->parentSpanId->equals($level2->context()->spanId));
        self::assertTrue($level2->context()->parentSpanId->equals($level1->context()->spanId));
        self::assertNull($level1->context()->parentSpanId);
    }

    public function test_exception_handling_in_trace() : void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());
        $tracer = $provider->tracer($this->resource, 'test');

        $exception = new \RuntimeException('Database connection failed');

        try {
            $tracer->trace('database-operation', function () use ($exception) : void {
                throw $exception;
            });
        } catch (\RuntimeException) {
        }

        self::assertCount(1, $processor->endedSpans());

        $span = $processor->endedSpans()[0];
        self::assertTrue($span->isEnded());
        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isError());
        self::assertSame('Database connection failed', $span->status()->description);

        self::assertCount(1, $span->events());
        $event = $span->events()[0];
        self::assertSame('exception', $event->name());
        self::assertSame(\RuntimeException::class, $event->attributes()['exception.type']);
    }

    public function test_multiple_tracers_share_trace_id() : void
    {
        $ctx = context();
        $storage = new MemoryContextStorage($ctx);
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), $storage);

        $httpTracer = $provider->tracer($this->resource, 'http-client', '1.0.0');
        $dbTracer = $provider->tracer($this->resource, 'database', '2.0.0');

        $httpSpan = $httpTracer->span('http-request');
        $dbSpan = $dbTracer->span('db-query');

        self::assertTrue($httpSpan->context()->traceId->equals($ctx->traceId));
        self::assertTrue($dbSpan->context()->traceId->equals($ctx->traceId));

        $httpTracer->complete($httpSpan);
        $dbTracer->complete($dbSpan);
    }

    public function test_provider_creates_new_tracer_each_time() : void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());

        $tracer1 = $provider->tracer($this->resource, 'my-lib', '1.0.0');
        $tracer2 = $provider->tracer($this->resource, 'my-lib', '1.0.0');

        self::assertNotSame($tracer1, $tracer2);
        self::assertSame($tracer1->name(), $tracer2->name());
        self::assertSame($tracer1->version(), $tracer2->version());
    }

    public function test_provider_flush() : void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());

        $tracer = $provider->tracer($this->resource, 'test');
        $span = $tracer->span('operation');
        $tracer->complete($span);

        self::assertTrue($processor->flush());
    }

    public function test_trace_helper_completes_span_on_success() : void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());
        $tracer = $provider->tracer($this->resource, 'test');

        $result = $tracer->trace('calculate', fn () => 42);

        self::assertSame(42, $result);
        self::assertCount(1, $processor->endedSpans());

        $span = $processor->endedSpans()[0];
        self::assertSame('calculate', $span->name());
        self::assertTrue($span->isEnded());
        self::assertNotNull($span->status());
        self::assertTrue($span->status()->isOk());
    }

    private function clock() : ClockInterface
    {
        $clock = $this->createMock(ClockInterface::class);
        $clock->method('now')->willReturn(new \DateTimeImmutable());

        return $clock;
    }

    private function createProcessor() : MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidSpanExporter());
    }
}
