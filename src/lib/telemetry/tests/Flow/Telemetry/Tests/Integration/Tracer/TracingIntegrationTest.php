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
        $rootScope = $tracer->activate($rootSpan);
        $rootSpan->setAttribute('http.method', 'POST');
        $rootSpan->setAttribute('http.url', '/api/orders');

        $dbSpan = $tracer->span('database-query', SpanKind::CLIENT);
        $dbScope = $tracer->activate($dbSpan);
        $dbSpan->setAttribute('db.system', 'postgresql');
        $dbSpan->setAttribute('db.statement', 'SELECT * FROM orders');
        $dbSpan->setStatus(SpanStatus::ok());
        $dbScope->detach();
        $tracer->complete($dbSpan);

        $rootSpan->setStatus(SpanStatus::ok());
        $rootScope->detach();
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
        $scope1 = $tracer->activate($level1);
        $level2 = $tracer->span('level-2');
        $scope2 = $tracer->activate($level2);
        $level3 = $tracer->span('level-3');
        $scope3 = $tracer->activate($level3);
        $level4 = $tracer->span('level-4');
        $scope4 = $tracer->activate($level4);

        $scope4->detach();
        $tracer->complete($level4);
        $scope3->detach();
        $tracer->complete($level3);
        $scope2->detach();
        $tracer->complete($level2);
        $scope1->detach();
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
        static::assertSame(RuntimeException::class, $span->attributes()['error.type']);

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
        $scopeA = $tracerA->activate($spanA);

        $activeSpanId = $storage->current()->activeSpanId();
        static::assertNotNull($activeSpanId);
        static::assertTrue($spanA->context()->spanId->equals($activeSpanId));

        $spanB = $tracerB->span('span-b');
        $scopeB = $tracerB->activate($spanB);
        $activeSpanIdAfterB = $storage->current()->activeSpanId();
        static::assertNotNull($activeSpanIdAfterB);
        static::assertTrue($spanB->context()->spanId->equals($activeSpanIdAfterB));

        $scopeB->detach();
        $tracerB->complete($spanB);

        $activeSpanIdAfterBComplete = $storage->current()->activeSpanId();
        static::assertNotNull($activeSpanIdAfterBComplete);
        static::assertTrue($spanA->context()->spanId->equals($activeSpanIdAfterBComplete));

        $scopeA->detach();
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
        $httpScope = $httpTracer->activate($httpSpan);
        $dbSpan = $dbTracer->span('db-query');
        $dbScope = $dbTracer->activate($dbSpan);

        static::assertTrue($dbSpan->context()->traceId->equals($httpSpan->context()->traceId));
        static::assertSame($httpSpan->context()->spanId->toHex(), $dbSpan->context()->parentSpanId?->toHex());

        $dbScope->detach();
        $dbTracer->complete($dbSpan);
        $httpScope->detach();
        $httpTracer->complete($httpSpan);
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

        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($span->status());
    }

    private function clock(): ClockInterface
    {
        $clock = $this->createStub(ClockInterface::class);
        $clock->method('now')->willReturn(new DateTimeImmutable());

        return $clock;
    }

    private function createProcessor(): MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidExporter());
    }
}
