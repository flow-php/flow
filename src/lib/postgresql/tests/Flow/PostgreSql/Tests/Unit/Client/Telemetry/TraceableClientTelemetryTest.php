<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryAttributes;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryConfig;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryOptions;
use Flow\PostgreSql\Client\Telemetry\TransactionSpanMode;
use Flow\PostgreSql\Explain\Plan\Cost;
use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Explain\Plan\PlanNode;
use Flow\PostgreSql\Explain\Plan\PlanNodeType;
use Flow\PostgreSql\Tests\Unit\Client\RowMapper\Fake\SpyRowMapper;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\SemConvAttributes;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use stdClass;

use function array_map;
use function count;
use function Flow\PostgreSql\DSL\pgsql_connection_params;
use function Flow\PostgreSql\DSL\postgresql_telemetry_config;
use function Flow\PostgreSql\DSL\postgresql_telemetry_options;
use function Flow\PostgreSql\DSL\traceable_postgresql_client;
use function Flow\Telemetry\DSL\logger_provider;
use function Flow\Telemetry\DSL\memory_context_storage;
use function Flow\Telemetry\DSL\memory_log_processor;
use function Flow\Telemetry\DSL\memory_metric_processor;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\meter_provider;
use function Flow\Telemetry\DSL\resource;
use function Flow\Telemetry\DSL\telemetry;
use function Flow\Telemetry\DSL\tracer_provider;
use function Flow\Telemetry\DSL\void_exporter;

final class TraceableClientTelemetryTest extends TestCase
{
    public function test_emits_no_spans_when_tracing_is_suppressed(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $clock = new SystemClock();
        $contextStorage = memory_context_storage(Context::root()->withSuppressedTracing());
        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage, new SuppressingSampler(new AlwaysOnSampler())),
            meter_provider(memory_metric_processor(void_exporter()), $clock),
            logger_provider(memory_log_processor(void_exporter()), $clock, $contextStorage),
        );
        $config = postgresql_telemetry_config(
            $tel,
            $clock,
            postgresql_telemetry_options(traceQueries: true, transactionSpans: TransactionSpanMode::GROUPED),
        );

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchAll')->willReturn([]);
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchAll('SELECT * FROM users');
        $client->execute('UPDATE users SET active = true WHERE id = $1', [1]);

        static::assertCount(
            0,
            $spanProcessor->endedSpans(),
            'PostgreSQL client instrumentation must emit no spans while tracing is suppressed',
        );
    }

    public function test_all_telemetry_signals_work_together(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $metricProcessor = memory_metric_processor(void_exporter());
        $logProcessor = memory_log_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            $metricProcessor,
            $logProcessor,
            postgresql_telemetry_options(
                traceQueries: true,
                transactionSpans: TransactionSpanMode::GROUPED,
                collectMetrics: true,
                logQueries: true,
            ),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient
            ->method('getTransactionNestingLevel')
            ->willReturnCallback(static function () use (&$nestingLevel): int {
                return $nestingLevel;
            });
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });
        $mockClient->method('fetchAll')->willReturn([['id' => 1], ['id' => 2]]);
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->transaction(static function (Client $c): void {
            $c->fetchAll('SELECT * FROM users');
            $c->execute('UPDATE users SET active = true WHERE id = $1', [1]);
        });

        $this->collectMetrics($config);

        static::assertCount(3, $spanProcessor->endedSpans(), 'Expected 3 spans: transaction + 2 queries');
        static::assertCount(2, $logProcessor->entries(), 'Expected 2 log entries for 2 queries');
        static::assertGreaterThan(0, $metricProcessor->countMetrics(), 'Expected metrics to be recorded');

        $durationMetrics = $metricProcessor->metricsWithName('db.client.operation.duration');
        static::assertGreaterThanOrEqual(3, count($durationMetrics), 'Expected at least 3 duration metrics');
    }

    public function test_begin_transaction_creates_span_when_tracing_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            options: postgresql_telemetry_options(transactionSpans: TransactionSpanMode::GROUPED),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient
            ->method('getTransactionNestingLevel')
            ->willReturnCallback(static function () use (&$nestingLevel): int {
                return $nestingLevel;
            });
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->commit();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('BEGIN TRANSACTION', $spans[0]->name());
        static::assertSame(1, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL]);
    }

    public function test_commit_completes_transaction_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            options: postgresql_telemetry_options(transactionSpans: TransactionSpanMode::GROUPED),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient
            ->method('getTransactionNestingLevel')
            ->willReturnCallback(static function () use (&$nestingLevel): int {
                return $nestingLevel;
            });
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->commit();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($spans[0]->status());
    }

    public function test_per_operation_mode_creates_a_short_span_for_each_transaction_call(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            options: postgresql_telemetry_options(transactionSpans: TransactionSpanMode::PER_OPERATION),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static fn(): int => $nestingLevel);
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->commit();

        $spans = $spanProcessor->endedSpans();
        static::assertSame(['BEGIN TRANSACTION', 'COMMIT TRANSACTION'], array_map(static fn($s) => $s->name(), $spans));
    }

    public function test_off_mode_creates_no_transaction_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            options: postgresql_telemetry_options(transactionSpans: TransactionSpanMode::OFF),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static fn(): int => $nestingLevel);
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->commit();

        static::assertCount(0, $spanProcessor->endedSpans());
    }

    public function test_close_completes_a_transaction_span_left_open(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            options: postgresql_telemetry_options(transactionSpans: TransactionSpanMode::GROUPED),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static fn(): int => $nestingLevel);
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();

        static::assertCount(
            0,
            $spanProcessor->endedSpans(),
            'grouped transaction span stays open until commit/rollBack',
        );

        $client->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('BEGIN TRANSACTION', $spans[0]->name());
        static::assertTrue($spans[0]->status()?->isError());
    }

    public function test_transaction_duration_metric_uses_operation_and_nesting_level(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $config = $this->createConfig(metricProcessor: $metricProcessor, options: postgresql_telemetry_options(
            transactionSpans: TransactionSpanMode::OFF,
            traceQueries: false,
            collectMetrics: true,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient
            ->method('getTransactionNestingLevel')
            ->willReturnCallback(static function () use (&$nestingLevel): int {
                return $nestingLevel;
            });
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->commit();

        $this->collectMetrics($config);

        $durations = $metricProcessor->metricsWithName('db.client.operation.duration');
        $operations = array_map(static fn($m) => $m->attributes->get('db.operation.name'), $durations);
        static::assertContains('begin', $operations);
        static::assertContains('commit', $operations);

        foreach ($durations as $metric) {
            static::assertSame('postgresql', $metric->attributes->get('db.system.name'));
            static::assertSame(1, $metric->attributes->get('flow.db.transaction.nesting_level'));
            static::assertFalse(
                $metric->attributes->has('server.address'),
                'server.address must not be a metric dimension',
            );
        }
    }

    public function test_duration_metric_is_recorded_when_metrics_enabled(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $config = $this->createConfig(metricProcessor: $metricProcessor, options: postgresql_telemetry_options(
            traceQueries: false,
            collectMetrics: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(5);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET active = true');

        $this->collectMetrics($config);

        $durationMetrics = $metricProcessor->metricsWithName('db.client.operation.duration');
        static::assertCount(1, $durationMetrics);
        static::assertSame(MetricType::HISTOGRAM, $durationMetrics[0]->type);
        static::assertGreaterThan(0, $durationMetrics[0]->value);
    }

    public function test_duration_metric_uses_low_cardinality_attributes(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $config = $this->createConfig(metricProcessor: $metricProcessor, options: postgresql_telemetry_options(
            traceQueries: false,
            collectMetrics: true,
            includeParameters: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET active = true WHERE id = $1', [42]);

        $this->collectMetrics($config);

        $attributes = $metricProcessor->metricsWithName('db.client.operation.duration')[0]->attributes;
        static::assertFalse($attributes->has('db.query.text'), 'query text must not be a metric dimension');
        static::assertFalse($attributes->has('db.query.parameter.0'), 'parameters must not be a metric dimension');
        static::assertSame('postgresql', $attributes->get('db.system.name'));
        static::assertSame('UPDATE', $attributes->get('db.operation.name'));
    }

    public function test_explain_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $plan = new Plan(new PlanNode(PlanNodeType::SEQ_SCAN, new Cost(0.0, 10.0), 100, 8));
        $mockClient = $this->createMockClient();
        $mockClient->method('explain')->willReturn($plan);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->explain('SELECT * FROM users WHERE id = $1', [1]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT users', $spans[0]->name());
    }

    public function test_fetch_all_into_creates_span_with_correct_row_count(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $users = [new stdClass(), new stdClass()];
        $mockClient = $this->createMockClient();
        $mockClient->method('fetchAllInto')->willReturn($users);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchAllInto(new SpyRowMapper(), 'SELECT * FROM users');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(2, $spans[0]->attributes()[SemConvAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_into_creates_span_with_correct_row_count(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $user = new stdClass();
        $user->id = 1;
        $mockClient = $this->createMockClient();
        $mockClient->method('fetchInto')->willReturn($user);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchInto(new SpyRowMapper(), 'SELECT * FROM users WHERE id = $1', [1]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(1, $spans[0]->attributes()[SemConvAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_one_into_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $user = new stdClass();
        $user->id = 1;
        $mockClient = $this->createMockClient();
        $mockClient->method('fetchOneInto')->willReturn($user);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchOneInto(new SpyRowMapper(), 'SELECT * FROM users WHERE id = $1', [1]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(1, $spans[0]->attributes()[SemConvAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_scalar_bool_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarBool')->willReturn(true);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarBool('SELECT active FROM users WHERE id = $1', [1]);

        static::assertTrue($result);
        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_fetch_scalar_float_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarFloat')->willReturn(99.5);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarFloat('SELECT price FROM products WHERE id = $1', [1]);

        static::assertSame(99.5, $result);
        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_fetch_scalar_int_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarInt')->willReturn(42);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarInt('SELECT COUNT(*) FROM users');

        static::assertSame(42, $result);
        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_fetch_scalar_string_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarString')->willReturn('John');

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarString('SELECT name FROM users WHERE id = $1', [1]);

        static::assertSame('John', $result);
        static::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_metrics_are_not_recorded_when_disabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $metricProcessor = memory_metric_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            $metricProcessor,
            options: postgresql_telemetry_options(traceQueries: true, collectMetrics: false),
        );

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(5);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET active = true');

        $this->collectMetrics($config);

        static::assertCount(0, $metricProcessor->metrics());
    }

    public function test_nested_transaction_creates_savepoint_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            options: postgresql_telemetry_options(transactionSpans: TransactionSpanMode::GROUPED),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient
            ->method('getTransactionNestingLevel')
            ->willReturnCallback(static function () use (&$nestingLevel): int {
                return $nestingLevel;
            });
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->beginTransaction();
        $client->commit();
        $client->commit();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(2, $spans);
        static::assertSame('BEGIN SAVEPOINT', $spans[0]->name());
        static::assertSame(
            'savepoint_1',
            $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_TRANSACTION_SAVEPOINT],
        );
        static::assertSame('BEGIN TRANSACTION', $spans[1]->name());
    }

    public function test_queries_are_logged_when_logging_enabled(): void
    {
        $logProcessor = memory_log_processor(void_exporter());
        $config = $this->createConfig(
            logProcessor: $logProcessor,
            options: postgresql_telemetry_options(logQueries: true),
        );

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(3);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('DELETE FROM users WHERE inactive = $1', [true]);

        $logs = $logProcessor->entries();
        static::assertCount(1, $logs);
        static::assertSame('Executing query', $logs[0]->record->body);
        static::assertSame(Severity::DEBUG, $logs[0]->record->severity);
        static::assertSame(
            'DELETE FROM users WHERE inactive = $1',
            $logs[0]->record->attributes->get(SemConvAttributes::DB_QUERY_TEXT),
        );
    }

    public function test_queries_are_not_logged_when_disabled(): void
    {
        $logProcessor = memory_log_processor(void_exporter());
        $config = $this->createConfig(
            logProcessor: $logProcessor,
            options: postgresql_telemetry_options(logQueries: false),
        );

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(3);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('DELETE FROM users WHERE inactive = $1', [true]);

        static::assertCount(0, $logProcessor->entries());
    }

    public function test_rollback_completes_all_nested_transaction_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            options: postgresql_telemetry_options(transactionSpans: TransactionSpanMode::GROUPED),
        );

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient
            ->method('getTransactionNestingLevel')
            ->willReturnCallback(static function () use (&$nestingLevel): int {
                return $nestingLevel;
            });
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('rollBack')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel = 0;
            });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->beginTransaction();
        $client->rollBack();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(2, $spans);
    }

    public function test_row_count_metric_is_recorded_for_fetch_operations(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $metricProcessor = memory_metric_processor(void_exporter());
        $config = $this->createConfig(
            $spanProcessor,
            $metricProcessor,
            options: postgresql_telemetry_options(traceQueries: true, collectMetrics: true),
        );

        $mockClient = $this->createMockClient();
        $mockClient
            ->method('fetchAll')
            ->willReturn([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchAll('SELECT * FROM users');

        $this->collectMetrics($config);

        $rowMetrics = $metricProcessor->metricsWithName('db.client.response.returned_rows');
        static::assertCount(1, $rowMetrics);
        static::assertSame(MetricType::HISTOGRAM, $rowMetrics[0]->type);
        static::assertSame(3.0, $rowMetrics[0]->value);
    }

    public function test_transaction_callback_creates_parent_span_with_query_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
            transactionSpans: TransactionSpanMode::GROUPED,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient
            ->method('getTransactionNestingLevel')
            ->willReturnCallback(static function () use (&$nestingLevel): int {
                return $nestingLevel;
            });
        $mockClient
            ->method('beginTransaction')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel++;
            });
        $mockClient
            ->method('commit')
            ->willReturnCallback(static function () use (&$nestingLevel): void {
                $nestingLevel--;
            });
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->transaction(static function (Client $c): void {
            $c->execute('INSERT INTO users (name) VALUES ($1)', ['John']);
        });

        $spans = $spanProcessor->endedSpans();
        static::assertCount(2, $spans);

        $spanNames = array_map(static fn($s) => $s->name(), $spans);
        static::assertContains('BEGIN TRANSACTION', $spanNames);
        static::assertContains('INSERT users', $spanNames);
    }

    private function collectMetrics(PostgreSqlTelemetryConfig $config): void
    {
        $meter = $config->telemetry->meter('flow_php_postgresql');

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
    }

    private function createConfig(
        ?MemorySpanProcessor $spanProcessor = null,
        ?MemoryMetricProcessor $metricProcessor = null,
        ?MemoryLogProcessor $logProcessor = null,
        ?PostgreSqlTelemetryOptions $options = null,
    ): PostgreSqlTelemetryConfig {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor ?? memory_span_processor(void_exporter()), $clock, $contextStorage),
            meter_provider($metricProcessor ?? memory_metric_processor(void_exporter()), $clock),
            logger_provider($logProcessor ?? memory_log_processor(void_exporter()), $clock, $contextStorage),
        );

        return postgresql_telemetry_config($tel, $clock, $options ?? postgresql_telemetry_options());
    }

    /**
     * @return Client&MockObject
     */
    private function createMockClient(): Client
    {
        $mockClient = $this->createMock(Client::class);
        $mockClient->method('parameters')->willReturn(pgsql_connection_params('testdb', 'localhost', 5432, 'user'));

        return $mockClient;
    }
}
