<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Telemetry;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Runtime\Telemetry\TraceableRowsStorage;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\Floe\Exception\IncompatibleSchemaException;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Double\ThrowingRemoveRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

#[CoversClass(TraceableRowsStorage::class)]
final class TraceableRowsStorageTest extends FlowTestCase
{
    private MemoryMetricProcessor $metricProcessor;

    private MemorySpanProcessor $spanProcessor;

    private Telemetry $telemetry;

    protected function setUp(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        $this->spanProcessor = new MemorySpanProcessor(new VoidExporter());
        $this->metricProcessor = new MemoryMetricProcessor(new VoidExporter());

        $this->telemetry = new Telemetry(
            Resource::create(['service.name' => 'test-service']),
            new TracerProvider($this->spanProcessor, $clock, $contextStorage),
            new MeterProvider($this->metricProcessor, $clock),
            new LoggerProvider(new MemoryLogProcessor(new VoidExporter()), $clock, $contextStorage),
        );
    }

    public function test_remove_creates_span_with_correct_name_and_attributes(): void
    {
        (new TraceableRowsStorage(new MemoryRowsStorage(), $this->telemetry))->remove('test-key');

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        static::assertCount(1, $spans);
        static::assertSame('cache.delete', $spans[0]->name());
        static::assertSame(SpanKind::CLIENT, $spans[0]->kind());
        static::assertSame('remove', $spans[0]->attributes()['cache.operation']);
        static::assertSame('test-key', $spans[0]->attributes()['cache.key']);
    }

    public function test_has_increments_hit_counter_when_key_exists(): void
    {
        $innerCache = new MemoryRowsStorage();
        $cache = new TraceableRowsStorage($innerCache, $this->telemetry, 'test_dataframe');

        $innerCache->append('existing-key', array_to_rows([[]], schema()));

        static::assertTrue($cache->has('existing-key'));

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('flow.cache.hits');
        $missMetrics = $this->metricProcessor->metricsWithName('flow.cache.misses');

        static::assertCount(1, $hitMetrics);
        static::assertCount(0, $missMetrics);
        static::assertSame(1, $hitMetrics[0]->value);
        static::assertSame('{operation}', $hitMetrics[0]->unit);
        static::assertSame('test_dataframe', $hitMetrics[0]->attributes->get('flow.etl.dataframe.name'));
    }

    public function test_has_increments_miss_counter_when_key_does_not_exist(): void
    {
        static::assertFalse((new TraceableRowsStorage(
            new MemoryRowsStorage(),
            $this->telemetry,
            'test_dataframe',
        ))->has('non-existing-key'));

        $this->telemetry->flush();
        $hitMetrics = $this->metricProcessor->metricsWithName('flow.cache.hits');
        $missMetrics = $this->metricProcessor->metricsWithName('flow.cache.misses');

        static::assertCount(0, $hitMetrics);
        static::assertCount(1, $missMetrics);
        static::assertSame(1, $missMetrics[0]->value);
        static::assertSame('test_dataframe', $missMetrics[0]->attributes->get('flow.etl.dataframe.name'));
    }

    public function test_reads_go_to_the_inner_storage(): void
    {
        $innerCache = new MemoryRowsStorage();
        $cache = new TraceableRowsStorage($innerCache, $this->telemetry);
        $innerCache->append('key', $rows = array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $cache->close('key');

        static::assertSame([$rows], iterator_to_array($cache->get('key'), false));
        static::assertSame(1, $cache->rows('key')->exactly());
        static::assertEquals($rows->schema(), $cache->schema('key'));
    }

    public function test_append_creates_span_with_rows_value_type(): void
    {
        (new TraceableRowsStorage(new MemoryRowsStorage(), $this->telemetry))->append('test-key', rows(schema()));

        $this->telemetry->flush();
        $spans = $this->spanProcessor->endedSpans();

        static::assertCount(1, $spans);
        static::assertSame('cache.set', $spans[0]->name());
        static::assertSame(SpanKind::CLIENT, $spans[0]->kind());
        static::assertSame('append', $spans[0]->attributes()['cache.operation']);
        static::assertSame('test-key', $spans[0]->attributes()['cache.key']);
        static::assertSame('Rows', $spans[0]->attributes()['cache.value_type']);
    }

    public function test_remove_records_exception_on_error(): void
    {
        $cache = new TraceableRowsStorage(new ThrowingRemoveRowsStorage(new MemoryRowsStorage()), $this->telemetry);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('spill clear failed');

        try {
            $cache->remove('test-key');
        } finally {
            $this->telemetry->flush();
            $spans = $this->spanProcessor->endedSpans();

            static::assertCount(1, $spans);
            static::assertTrue($spans[0]->status()?->isError());
            static::assertSame(RuntimeException::class, $spans[0]->attributes()['error.type']);
        }
    }

    public function test_append_records_exception_on_error(): void
    {
        $cache = new TraceableRowsStorage(
            new FilesystemRowsStorage(memory_filesystem(), path('memory://traceable'), new PhpBackend()),
            $this->telemetry,
        );
        $cache->append('test-key', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $this->expectException(IncompatibleSchemaException::class);

        try {
            $cache->append('test-key', array_to_rows([['name' => 'x']], schema(str_schema('name'))));
        } finally {
            $this->telemetry->flush();
            $span = $this->spanProcessor->endedSpans()[1];
            $status = $span->status();

            static::assertNotNull($status);
            static::assertTrue($status->isError());
            static::assertSame(IncompatibleSchemaException::class, $span->attributes()['error.type']);
            static::assertCount(1, $span->events());
        }
    }
}
