<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Telemetry;

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn, pgsql_mapper, postgresql_telemetry_config, postgresql_telemetry_options, traceable_postgresql_client};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryAttributes;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use PHPUnit\Framework\TestCase;

final class TraceableClientTest extends TestCase
{
    private Client $baseClient;

    protected function setUp() : void
    {
        if (!\extension_loaded('pgsql')) {
            self::markTestSkipped('ext-pgsql is not available');
        }

        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            self::markTestSkipped('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->baseClient = pgsql_client(
            pgsql_connection_dsn($dsn),
            mapper: pgsql_mapper(),
        );
    }

    protected function tearDown() : void
    {
        if (isset($this->baseClient)) {
            $this->baseClient->close();
        }
    }

    public function test_cursor_iteration_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceQueries: true,
        ));
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('CREATE TEMP TABLE test_cursor (id serial PRIMARY KEY, name text)');
        $client->execute('INSERT INTO test_cursor (name) VALUES ($1), ($2)', ['John', 'Jane']);

        $cursor = $client->cursor('SELECT * FROM test_cursor');
        $rows = [];

        foreach ($cursor->iterate() as $row) {
            $rows[] = $row;
        }

        self::assertCount(2, $rows);

        $spans = $spanProcessor->endedSpans();
        $cursorSpan = \array_filter($spans, static fn ($s) => \str_contains($s->name(), 'cursor'));
        self::assertNotEmpty($cursorSpan);

        $cursorSpan = \array_values($cursorSpan)[0];
        self::assertSame(2, $cursorSpan->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_execute_creates_span_with_database_attributes() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('CREATE TEMP TABLE test_execute_span (id serial PRIMARY KEY, name text)');
        $client->execute('INSERT INTO test_execute_span (name) VALUES ($1)', ['John']);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(2, $spans);

        $insertSpan = $spans[1];
        self::assertSame('flow.postgresql.INSERT test_execute_span', $insertSpan->name());
        self::assertSame('postgresql', $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_SYSTEM_NAME]);
        self::assertSame('INSERT', $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_OPERATION_NAME]);
        self::assertSame('test_execute_span', $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_COLLECTION_NAME]);
        self::assertArrayHasKey(PostgreSqlTelemetryAttributes::DB_NAMESPACE, $insertSpan->attributes());
        self::assertArrayHasKey(PostgreSqlTelemetryAttributes::SERVER_ADDRESS, $insertSpan->attributes());
    }

    public function test_failed_query_records_error_in_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $client = traceable_postgresql_client($this->baseClient, $config);

        try {
            $client->execute('SELECT * FROM nonexistent_table_12345');
        } catch (\Throwable) {
        }

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isError());
        self::assertArrayHasKey(PostgreSqlTelemetryAttributes::ERROR_TYPE, $spans[0]->attributes());
    }

    public function test_fetch_creates_span_with_row_count() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('CREATE TEMP TABLE test_fetch_span (id serial PRIMARY KEY, name text)');
        $client->execute('INSERT INTO test_fetch_span (name) VALUES ($1), ($2), ($3)', ['John', 'Jane', 'Bob']);
        $result = $client->fetchAll('SELECT * FROM test_fetch_span');

        self::assertCount(3, $result);

        $spans = $spanProcessor->endedSpans();
        $selectSpan = \array_filter($spans, static fn ($s) => \str_contains($s->name(), 'SELECT'));
        $selectSpan = \array_values($selectSpan)[0];

        self::assertSame(3, $selectSpan->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_logging_records_query_execution() : void
    {
        $logProcessor = memory_log_processor(void_log_exporter());
        $config = $this->createConfig(logProcessor: $logProcessor, options: postgresql_telemetry_options(
            logQueries: true,
        ));
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('CREATE TEMP TABLE test_logging (id serial PRIMARY KEY)');
        $client->execute('INSERT INTO test_logging DEFAULT VALUES');

        $logs = $logProcessor->entries();
        self::assertCount(2, $logs);
        self::assertSame('Executing query', $logs[0]->record->body);
        self::assertSame(Severity::DEBUG, $logs[0]->record->severity);
    }

    public function test_metrics_record_operation_duration() : void
    {
        $metricProcessor = memory_metric_processor(void_metric_exporter());
        $config = $this->createConfig(metricProcessor: $metricProcessor, options: postgresql_telemetry_options(
            collectMetrics: true,
        ));
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('SELECT 1');

        $this->collectMetrics($config);

        $durationMetrics = $metricProcessor->metricsWithName('db.client.operation.duration');
        self::assertCount(1, $durationMetrics);
        self::assertSame(MetricType::HISTOGRAM, $durationMetrics[0]->type);
        self::assertGreaterThan(0, $durationMetrics[0]->value);
    }

    public function test_nested_transaction_creates_savepoint_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
            traceQueries: false,
        ));
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('CREATE TEMP TABLE test_nested_tx (id serial PRIMARY KEY)');
        $client->transaction(static function (Client $outer) : void {
            $outer->execute('INSERT INTO test_nested_tx DEFAULT VALUES');

            $outer->transaction(static function (Client $inner) : void {
                $inner->execute('INSERT INTO test_nested_tx DEFAULT VALUES');
            });
        });

        $spans = $spanProcessor->endedSpans();
        self::assertCount(2, $spans);

        $spanNames = \array_map(static fn ($s) => $s->name(), $spans);
        self::assertContains('flow.postgresql.BEGIN TRANSACTION', $spanNames);
        self::assertContains('flow.postgresql.BEGIN SAVEPOINT', $spanNames);
    }

    public function test_parameters_are_included_when_enabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            includeParameters: true,
        ));
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('CREATE TEMP TABLE test_params (id serial PRIMARY KEY, name text)');
        $client->execute('INSERT INTO test_params (name) VALUES ($1)', ['John']);

        $spans = $spanProcessor->endedSpans();
        $insertSpan = \array_filter($spans, static fn ($s) => \str_contains($s->name(), 'INSERT'));
        $insertSpan = \array_values($insertSpan)[0];

        self::assertSame('John', $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . '1']);
    }

    public function test_transaction_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
            traceQueries: false,
        ));
        $client = traceable_postgresql_client($this->baseClient, $config);

        $client->execute('CREATE TEMP TABLE test_tx_span (id serial PRIMARY KEY)');
        $client->transaction(static function (Client $c) : void {
            $c->execute('INSERT INTO test_tx_span DEFAULT VALUES');
        });

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('flow.postgresql.BEGIN TRANSACTION', $spans[0]->name());
        self::assertSame(1, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL]);
    }

    private function collectMetrics($config) : void
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
        $options = null,
    ) {
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
}
