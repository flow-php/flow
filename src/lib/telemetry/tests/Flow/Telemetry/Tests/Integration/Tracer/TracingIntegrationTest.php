<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Integration\Tracer;

use DateTimeImmutable;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;
use RuntimeException;

use function Flow\Telemetry\DSL\context;

final class TracingIntegrationTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_complete_tracing_workflow(): void
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

        static::assertCount(2, $processor->endedSpans());

        $endedDbSpan = $processor->endedSpans()[0];
        static::assertSame('database-query', $endedDbSpan->name());
        static::assertSame(SpanKind::CLIENT, $endedDbSpan->kind());
        $endedDbSpanParentId = $endedDbSpan->context()->parentSpanId;
        static::assertNotNull($endedDbSpanParentId);
        static::assertTrue($endedDbSpanParentId->equals($rootSpan->context()->spanId));

        $endedRootSpan = $processor->endedSpans()[1];
        static::assertSame('handle-request', $endedRootSpan->name());
        static::assertSame(SpanKind::SERVER, $endedRootSpan->kind());
        static::assertTrue($endedRootSpan->context()->isRoot());
    }

    public function test_deeply_nested_spans(): void
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

        static::assertCount(4, $processor->endedSpans());

        $level4ParentId = $level4->context()->parentSpanId;
        $level3ParentId = $level3->context()->parentSpanId;
        $level2ParentId = $level2->context()->parentSpanId;
        static::assertNotNull($level4ParentId);
        static::assertNotNull($level3ParentId);
        static::assertNotNull($level2ParentId);
        static::assertTrue($level4ParentId->equals($level3->context()->spanId));
        static::assertTrue($level3ParentId->equals($level2->context()->spanId));
        static::assertTrue($level2ParentId->equals($level1->context()->spanId));
        static::assertNull($level1->context()->parentSpanId);
    }

    public function test_exception_handling_in_trace(): void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());
        $tracer = $provider->tracer($this->resource, 'test');

        $exception = new RuntimeException('Database connection failed');

        try {
            $tracer->trace('database-operation', static function () use ($exception): void {
                throw $exception;
            });
        } catch (RuntimeException) {
        }

        static::assertCount(1, $processor->endedSpans());

        $span = $processor->endedSpans()[0];
        static::assertTrue($span->isEnded());
        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertSame('Database connection failed', $status->description);

        static::assertCount(1, $span->events());
        $event = $span->events()[0];
        static::assertSame('exception', $event->name());
        static::assertSame(RuntimeException::class, $event->attributes()['exception.type']);
    }

    public function test_multiple_tracers_completing_spans_independently_preserve_context(): void
    {
        $storage = new MemoryContextStorage();
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), $storage);

        $tracerA = $provider->tracer($this->resource, 'tracer-a', '1.0.0');
        $tracerB = $provider->tracer($this->resource, 'tracer-b', '1.0.0');

        $spanA = $tracerA->span('span-a');

        $activeSpanId = $storage->current()->activeSpanId();
        static::assertNotNull($activeSpanId);
        static::assertTrue($spanA->context()->spanId->equals($activeSpanId));

        $spanB = $tracerB->span('span-b');
        $activeSpanIdAfterB = $storage->current()->activeSpanId();
        static::assertNotNull($activeSpanIdAfterB);
        static::assertTrue($spanB->context()->spanId->equals($activeSpanIdAfterB));

        $tracerB->complete($spanB);

        $activeSpanIdAfterBComplete = $storage->current()->activeSpanId();
        static::assertNotNull($activeSpanIdAfterBComplete);
        static::assertTrue($spanA->context()->spanId->equals($activeSpanIdAfterBComplete));

        $tracerA->complete($spanA);

        static::assertNull($storage->current()->activeSpanId());
    }

    public function test_multiple_tracers_share_trace_id(): void
    {
        $ctx = context();
        $storage = new MemoryContextStorage($ctx);
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), $storage);

        $httpTracer = $provider->tracer($this->resource, 'http-client', '1.0.0');
        $dbTracer = $provider->tracer($this->resource, 'database', '2.0.0');

        $httpSpan = $httpTracer->span('http-request');
        $dbSpan = $dbTracer->span('db-query');

        static::assertTrue($httpSpan->context()->traceId->equals($ctx->traceId));
        static::assertTrue($dbSpan->context()->traceId->equals($ctx->traceId));

        $httpTracer->complete($httpSpan);
        $dbTracer->complete($dbSpan);
    }

    public function test_provider_creates_new_tracer_each_time(): void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());

        $tracer1 = $provider->tracer($this->resource, 'my-lib', '1.0.0');
        $tracer2 = $provider->tracer($this->resource, 'my-lib', '1.0.0');

        static::assertNotSame($tracer1, $tracer2);
        static::assertSame($tracer1->name(), $tracer2->name());
        static::assertSame($tracer1->version(), $tracer2->version());
    }

    public function test_provider_flush(): void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());

        $tracer = $provider->tracer($this->resource, 'test');
        $span = $tracer->span('operation');
        $tracer->complete($span);

        static::assertTrue($processor->flush());
    }

    public function test_trace_helper_completes_span_on_success(): void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, $this->clock(), new MemoryContextStorage());
        $tracer = $provider->tracer($this->resource, 'test');

        $result = $tracer->trace('calculate', static fn() => 42);

        static::assertSame(42, $result);
        static::assertCount(1, $processor->endedSpans());

        $span = $processor->endedSpans()[0];
        static::assertSame('calculate', $span->name());
        static::assertTrue($span->isEnded());
        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    private function clock(): ClockInterface
    {
        $clock = $this->createMock(ClockInterface::class);
        $clock->method('now')->willReturn(new DateTimeImmutable());

        return $clock;
    }

    private function createProcessor(): MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidExporter());
    }
}
