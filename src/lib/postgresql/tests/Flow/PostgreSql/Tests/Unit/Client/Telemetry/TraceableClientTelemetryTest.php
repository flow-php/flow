<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use function Flow\PostgreSql\DSL\{pgsql_connection_params, postgresql_telemetry_config, postgresql_telemetry_options, traceable_postgresql_client};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Telemetry\{PostgreSqlTelemetryAttributes, PostgreSqlTelemetryConfig, PostgreSqlTelemetryOptions};
use Flow\PostgreSql\Explain\Plan\{Cost, Plan, PlanNode, PlanNodeType};
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

final class TraceableClientTelemetryTest extends TestCase
{
    public function test_all_telemetry_signals_work_together() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $metricProcessor = memory_metric_processor(void_metric_exporter());
        $logProcessor = memory_log_processor(void_log_exporter());
        $config = $this->createConfig($spanProcessor, $metricProcessor, $logProcessor, postgresql_telemetry_options(
            traceQueries: true,
            traceTransactions: true,
            collectMetrics: true,
            logQueries: true,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static function () use (&$nestingLevel) : int {
            return $nestingLevel;
        });
        $mockClient->method('beginTransaction')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel++;
        });
        $mockClient->method('commit')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel--;
        });
        $mockClient->method('fetchAll')->willReturn([['id' => 1], ['id' => 2]]);
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->transaction(static function (Client $c) : void {
            $c->fetchAll('SELECT * FROM users');
            $c->execute('UPDATE users SET active = true WHERE id = $1', [1]);
        });

        $this->collectMetrics($config);

        self::assertCount(3, $spanProcessor->endedSpans(), 'Expected 3 spans: transaction + 2 queries');
        self::assertCount(2, $logProcessor->entries(), 'Expected 2 log entries for 2 queries');
        self::assertGreaterThan(0, $metricProcessor->countMetrics(), 'Expected metrics to be recorded');

        $durationMetrics = $metricProcessor->metricsWithName('db.client.operation.duration');
        self::assertGreaterThanOrEqual(3, \count($durationMetrics), 'Expected at least 3 duration metrics');
    }

    public function test_begin_transaction_creates_span_when_tracing_enabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static function () use (&$nestingLevel) : int {
            return $nestingLevel;
        });
        $mockClient->method('beginTransaction')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel++;
        });
        $mockClient->method('commit')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel--;
        });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->commit();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('BEGIN TRANSACTION', $spans[0]->name());
        self::assertSame(1, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL]);
    }

    public function test_commit_completes_transaction_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static function () use (&$nestingLevel) : int {
            return $nestingLevel;
        });
        $mockClient->method('beginTransaction')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel++;
        });
        $mockClient->method('commit')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel--;
        });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->commit();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertNotNull($spans[0]->status());
        self::assertFalse($spans[0]->status()->isError());
    }

    public function test_duration_metric_is_recorded_when_metrics_enabled() : void
    {
        $metricProcessor = memory_metric_processor(void_metric_exporter());
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
        self::assertCount(1, $durationMetrics);
        self::assertSame(MetricType::HISTOGRAM, $durationMetrics[0]->type);
        self::assertGreaterThan(0, $durationMetrics[0]->value);
    }

    public function test_explain_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $plan = new Plan(
            new PlanNode(
                PlanNodeType::SEQ_SCAN,
                new Cost(0.0, 10.0),
                100,
                8,
            ),
        );
        $mockClient = $this->createMockClient();
        $mockClient->method('explain')->willReturn($plan);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->explain('SELECT * FROM users WHERE id = $1', [1]);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SELECT users', $spans[0]->name());
    }

    public function test_fetch_all_into_creates_span_with_correct_row_count() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $users = [new \stdClass(), new \stdClass()];
        $mockClient = $this->createMockClient();
        $mockClient->method('fetchAllInto')->willReturn($users);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchAllInto(\stdClass::class, 'SELECT * FROM users');

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(2, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_into_creates_span_with_correct_row_count() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $user = new \stdClass();
        $user->id = 1;
        $mockClient = $this->createMockClient();
        $mockClient->method('fetchInto')->willReturn($user);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchInto(\stdClass::class, 'SELECT * FROM users WHERE id = $1', [1]);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(1, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_one_into_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $user = new \stdClass();
        $user->id = 1;
        $mockClient = $this->createMockClient();
        $mockClient->method('fetchOneInto')->willReturn($user);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchOneInto(\stdClass::class, 'SELECT * FROM users WHERE id = $1', [1]);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(1, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_scalar_bool_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarBool')->willReturn(true);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarBool('SELECT active FROM users WHERE id = $1', [1]);

        self::assertTrue($result);
        self::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_fetch_scalar_float_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarFloat')->willReturn(99.5);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarFloat('SELECT price FROM products WHERE id = $1', [1]);

        self::assertSame(99.5, $result);
        self::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_fetch_scalar_int_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarInt')->willReturn(42);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarInt('SELECT COUNT(*) FROM users');

        self::assertSame(42, $result);
        self::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_fetch_scalar_string_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchScalarString')->willReturn('John');

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchScalarString('SELECT name FROM users WHERE id = $1', [1]);

        self::assertSame('John', $result);
        self::assertCount(1, $spanProcessor->endedSpans());
    }

    public function test_metrics_are_not_recorded_when_disabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $metricProcessor = memory_metric_processor(void_metric_exporter());
        $config = $this->createConfig($spanProcessor, $metricProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
            collectMetrics: false,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(5);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET active = true');

        $this->collectMetrics($config);

        self::assertCount(0, $metricProcessor->metrics());
    }

    public function test_nested_transaction_creates_savepoint_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static function () use (&$nestingLevel) : int {
            return $nestingLevel;
        });
        $mockClient->method('beginTransaction')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel++;
        });
        $mockClient->method('commit')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel--;
        });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->beginTransaction();
        $client->commit();
        $client->commit();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(2, $spans);
        self::assertSame('BEGIN SAVEPOINT', $spans[0]->name());
        self::assertSame('savepoint_1', $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_TRANSACTION_SAVEPOINT]);
        self::assertSame('BEGIN TRANSACTION', $spans[1]->name());
    }

    public function test_queries_are_logged_when_logging_enabled() : void
    {
        $logProcessor = memory_log_processor(void_log_exporter());
        $config = $this->createConfig(logProcessor: $logProcessor, options: postgresql_telemetry_options(
            logQueries: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(3);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('DELETE FROM users WHERE inactive = $1', [true]);

        $logs = $logProcessor->entries();
        self::assertCount(1, $logs);
        self::assertSame('Executing query', $logs[0]->record->body);
        self::assertSame(Severity::DEBUG, $logs[0]->record->severity);
        self::assertSame('DELETE FROM users WHERE inactive = $1', $logs[0]->record->attributes->get(PostgreSqlTelemetryAttributes::DB_QUERY_TEXT));
    }

    public function test_queries_are_not_logged_when_disabled() : void
    {
        $logProcessor = memory_log_processor(void_log_exporter());
        $config = $this->createConfig(logProcessor: $logProcessor, options: postgresql_telemetry_options(
            logQueries: false,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(3);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('DELETE FROM users WHERE inactive = $1', [true]);

        self::assertCount(0, $logProcessor->entries());
    }

    public function test_rollback_completes_all_nested_transaction_spans() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static function () use (&$nestingLevel) : int {
            return $nestingLevel;
        });
        $mockClient->method('beginTransaction')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel++;
        });
        $mockClient->method('rollBack')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel = 0;
        });

        $client = traceable_postgresql_client($mockClient, $config);
        $client->beginTransaction();
        $client->beginTransaction();
        $client->rollBack();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(2, $spans);
    }

    public function test_row_count_metric_is_recorded_for_fetch_operations() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $metricProcessor = memory_metric_processor(void_metric_exporter());
        $config = $this->createConfig($spanProcessor, $metricProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
            collectMetrics: true,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('fetchAll')->willReturn([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
        ]);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->fetchAll('SELECT * FROM users');

        $this->collectMetrics($config);

        $rowMetrics = $metricProcessor->metricsWithName('db.client.response.returned_rows');
        self::assertCount(1, $rowMetrics);
        self::assertSame(MetricType::HISTOGRAM, $rowMetrics[0]->type);
        self::assertSame(3.0, $rowMetrics[0]->value);
    }

    public function test_transaction_callback_creates_parent_span_with_query_spans() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
            traceTransactions: true,
        ));

        $nestingLevel = 0;
        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturnCallback(static function () use (&$nestingLevel) : int {
            return $nestingLevel;
        });
        $mockClient->method('beginTransaction')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel++;
        });
        $mockClient->method('commit')->willReturnCallback(static function () use (&$nestingLevel) : void {
            $nestingLevel--;
        });
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->transaction(static function (Client $c) : void {
            $c->execute('INSERT INTO users (name) VALUES ($1)', ['John']);
        });

        $spans = $spanProcessor->endedSpans();
        self::assertCount(2, $spans);

        $spanNames = \array_map(static fn ($s) => $s->name(), $spans);
        self::assertContains('BEGIN TRANSACTION', $spanNames);
        self::assertContains('INSERT users', $spanNames);
    }

    private function collectMetrics(PostgreSqlTelemetryConfig $config) : void
    {
        $meter = $config->telemetry->meter('flow.postgresql');

        foreach ($meter->collect() as $metric) {
            $meter->processor()->process($metric);
        }
    }

    private function createConfig(
        ?MemorySpanProcessor $spanProcessor = null,
        ?MemoryMetricProcessor $metricProcessor = null,
        ?MemoryLogProcessor $logProcessor = null,
        ?PostgreSqlTelemetryOptions $options = null,
    ) : PostgreSqlTelemetryConfig {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor ?? memory_span_processor(void_span_exporter()), $clock, $contextStorage),
            meter_provider($metricProcessor ?? memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider($logProcessor ?? memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );

        return postgresql_telemetry_config($tel, $clock, $options ?? postgresql_telemetry_options());
    }

    /**
     * @return Client&MockObject
     */
    private function createMockClient() : Client
    {
        $mockClient = $this->createMock(Client::class);
        $mockClient->method('parameters')->willReturn(
            pgsql_connection_params('testdb', 'localhost', 5432, 'user')
        );

        return $mockClient;
    }
}
