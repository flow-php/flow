<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Doctrine\DBAL;

use Doctrine\DBAL\Driver\Exception as DriverException;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\QueryTracer;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\SqlAttributes;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function hrtime;

#[CoversClass(QueryTracer::class)]
final class QueryTracerTest extends TestCase
{
    #[TestWith(['SELECT', 'users', 'SELECT users'])]
    #[TestWith(['SELECT', null, 'SELECT'])]
    #[TestWith([null, null, 'query'])]
    public function test_span_name(?string $operation, ?string $collection, string $expected): void
    {
        $queryTracer = $this->queryTracer($this->createTelemetry(new MemorySpanProcessor(new MemoryExporter())));

        static::assertSame($expected, $queryTracer->spanName(new SqlAttributes($operation, $collection)));
    }

    public function test_extract_delegates_to_the_sql_extractor(): void
    {
        $queryTracer = $this->queryTracer($this->createTelemetry(new MemorySpanProcessor(new MemoryExporter())));

        $attributes = $queryTracer->extract('SELECT * FROM users');

        static::assertSame('SELECT', $attributes->operation);
        static::assertSame('users', $attributes->collection);
    }

    public function test_record_duration_noop_when_start_time_is_false(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry(new MemorySpanProcessor(new MemoryExporter()), $metricProcessor);
        $queryTracer = $this->queryTracer($telemetry, collectMetrics: true);

        $queryTracer->recordDuration(false, ['db.operation.name' => 'begin']);
        $this->collectMetrics($telemetry);

        static::assertSame(0, $metricProcessor->countMetrics());
    }

    public function test_query_attributes_merge_base_operation_collection_and_query_text(): void
    {
        $queryTracer = $this->queryTracer(
            $this->createTelemetry(new MemorySpanProcessor(new MemoryExporter())),
            baseAttributes: ['db.system.name' => 'postgresql', 'db.namespace' => 'app', 'server.port' => 5432],
        );

        $attributes = $queryTracer->queryAttributes('SELECT * FROM users', new SqlAttributes('SELECT', 'users'));

        static::assertSame(
            [
                'db.system.name' => 'postgresql',
                'db.namespace' => 'app',
                'server.port' => 5432,
                'db.operation.name' => 'SELECT',
                'db.collection.name' => 'users',
                'db.query.text' => 'SELECT * FROM users',
            ],
            $attributes,
        );
    }

    public function test_query_attributes_omit_operation_and_collection_when_absent(): void
    {
        $queryTracer = $this->queryTracer($this->createTelemetry(new MemorySpanProcessor(new MemoryExporter())));

        $attributes = $queryTracer->queryAttributes('BADSQL', new SqlAttributes(null, null));

        static::assertArrayNotHasKey('db.operation.name', $attributes);
        static::assertArrayNotHasKey('db.collection.name', $attributes);
        static::assertSame('BADSQL', $attributes['db.query.text']);
    }

    public function test_query_attributes_truncate_query_text(): void
    {
        $queryTracer = $this->queryTracer(
            $this->createTelemetry(new MemorySpanProcessor(new MemoryExporter())),
            maxSqlLength: 6,
        );

        $attributes = $queryTracer->queryAttributes('SELECT 1', new SqlAttributes('SELECT', null));

        static::assertSame('SELECT...', $attributes['db.query.text']);
    }

    public function test_parameter_attributes_empty_when_disabled(): void
    {
        $queryTracer = $this->queryTracer($this->createTelemetry(new MemorySpanProcessor(new MemoryExporter())));

        static::assertSame([], $queryTracer->parameterAttributes([1, 2]));
    }

    public function test_parameter_attributes_formatted_and_capped_when_enabled(): void
    {
        $queryTracer = $this->queryTracer(
            $this->createTelemetry(new MemorySpanProcessor(new MemoryExporter())),
            includeParameters: true,
            maxParameters: 2,
        );

        static::assertSame(
            [
                'db.query.parameter.0' => '1',
                'db.query.parameter.1' => '2',
            ],
            $queryTracer->parameterAttributes([1, 2, 3]),
        );
    }

    public function test_record_error_sets_error_type_and_status(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $queryTracer = $this->queryTracer($this->createTelemetry($spanProcessor));

        $span = $this->openSpan($queryTracer);
        $queryTracer->recordError($span, new RuntimeException('boom'));
        $queryTracer->tracer()->complete($span);

        static::assertTrue($span->status()?->isError());
        static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
        static::assertArrayNotHasKey('db.response.status_code', $span->attributes());
    }

    public function test_record_error_sets_status_code_from_driver_exception(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $queryTracer = $this->queryTracer($this->createTelemetry($spanProcessor));

        $exception = $this->createStub(DriverException::class);
        $exception->method('getSQLState')->willReturn('23505');

        $span = $this->openSpan($queryTracer);
        $queryTracer->recordError($span, $exception);
        $queryTracer->tracer()->complete($span);

        static::assertSame('23505', $span->attributes()['db.response.status_code']);
    }

    public function test_record_query_metrics_uses_low_cardinality_attributes(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry(new MemorySpanProcessor(new MemoryExporter()), $metricProcessor);
        $queryTracer = $this->queryTracer(
            $telemetry,
            baseAttributes: ['db.system.name' => 'postgresql', 'db.namespace' => 'app'],
            collectMetrics: true,
        );

        $queryTracer->recordQueryMetrics(hrtime(true), 5, new SqlAttributes('SELECT', 'users'));
        $this->collectMetrics($telemetry);

        $duration = $metricProcessor->metricsWithName('db.client.operation.duration');
        static::assertCount(1, $duration);
        static::assertSame('SELECT', $duration[0]->attributes->get('db.operation.name'));
        static::assertSame('users', $duration[0]->attributes->get('db.collection.name'));
        static::assertFalse($duration[0]->attributes->has('db.query.text'));

        static::assertCount(1, $metricProcessor->metricsWithName('db.client.response.returned_rows'));
    }

    public function test_record_query_metrics_noop_when_metrics_disabled(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry(new MemorySpanProcessor(new MemoryExporter()), $metricProcessor);
        $queryTracer = $this->queryTracer($telemetry, collectMetrics: false);

        $queryTracer->recordQueryMetrics(hrtime(true), 5, new SqlAttributes('SELECT', 'users'));
        $this->collectMetrics($telemetry);

        static::assertSame(0, $metricProcessor->countMetrics());
    }

    public function test_record_duration_records_given_attributes(): void
    {
        $metricProcessor = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = $this->createTelemetry(new MemorySpanProcessor(new MemoryExporter()), $metricProcessor);
        $queryTracer = $this->queryTracer($telemetry, collectMetrics: true);

        $queryTracer->recordDuration(hrtime(true), ['db.operation.name' => 'begin']);
        $this->collectMetrics($telemetry);

        $duration = $metricProcessor->metricsWithName('db.client.operation.duration');
        static::assertCount(1, $duration);
        static::assertSame('begin', $duration[0]->attributes->get('db.operation.name'));
    }

    private function openSpan(QueryTracer $queryTracer): Span
    {
        return $queryTracer->tracer()->span('test', SpanKind::CLIENT);
    }

    /**
     * @param array<string, int|string> $baseAttributes
     */
    private function queryTracer(
        Telemetry $telemetry,
        array $baseAttributes = [],
        int $maxSqlLength = 1000,
        bool $collectMetrics = false,
        bool $includeParameters = false,
        int $maxParameters = 10,
        int $maxParameterLength = 100,
    ): QueryTracer {
        return new QueryTracer(
            $telemetry,
            $baseAttributes,
            $maxSqlLength,
            $collectMetrics,
            $includeParameters,
            $maxParameters,
            $maxParameterLength,
        );
    }

    private function createTelemetry(
        MemorySpanProcessor $spanProcessor,
        ?MemoryMetricProcessor $metricProcessor = null,
    ): Telemetry {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor ?? new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }

    private function collectMetrics(Telemetry $telemetry): void
    {
        $meter = $telemetry->meter('flow.symfony.dbal', PackageVersion::get('doctrine/dbal'));

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
    }
}
