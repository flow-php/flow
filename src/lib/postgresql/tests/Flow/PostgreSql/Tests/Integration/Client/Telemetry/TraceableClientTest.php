<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client\Telemetry;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryAttributes;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryConfig;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryOptions;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;

use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\postgresql_telemetry_config;
use function Flow\PostgreSql\DSL\postgresql_telemetry_options;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
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

final class TraceableClientTest extends PostgreSqlTestCase
{
    public function test_cursor_iteration_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(traceQueries: true));
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(
            create()
                ->temporaryTable('test_cursor')
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_text()))
                ->constraint(primary_key('id')),
        );
        $client->execute(
            insert()->into('test_cursor')->columns('name')->values(param(1))->values(param(2)),
            ['John', 'Jane'],
        );

        $cursor = $client->cursor(select(star())->from(table('test_cursor')));
        $rows = [];

        foreach ($cursor->iterate() as $row) {
            $rows[] = $row;
        }

        static::assertCount(2, $rows);

        $spans = $spanProcessor->endedSpans();
        $cursorSpan = \array_filter($spans, static fn($s) => \str_contains($s->name(), 'cursor'));
        static::assertNotEmpty($cursorSpan);

        $cursorSpan = \array_values($cursorSpan)[0];
        static::assertSame(2, $cursorSpan->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_execute_creates_span_with_database_attributes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(
            create()
                ->temporaryTable('test_execute_span')
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_text()))
                ->constraint(primary_key('id')),
        );
        $client->execute(insert()->into('test_execute_span')->columns('name')->values(param(1)), ['John']);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(2, $spans);

        $insertSpan = $spans[1];
        static::assertSame('INSERT test_execute_span', $insertSpan->name());
        static::assertSame('postgresql', $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_SYSTEM_NAME]);
        static::assertSame('INSERT', $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_OPERATION_NAME]);
        static::assertSame(
            'test_execute_span',
            $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_COLLECTION_NAME],
        );
        static::assertArrayHasKey(PostgreSqlTelemetryAttributes::DB_NAMESPACE, $insertSpan->attributes());
        static::assertArrayHasKey(PostgreSqlTelemetryAttributes::SERVER_ADDRESS, $insertSpan->attributes());
    }

    public function test_failed_query_records_error_in_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        try {
            $client->execute(select(star())->from(table('nonexistent_table_12345')));
        } catch (\Throwable) {
        }

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertArrayHasKey(PostgreSqlTelemetryAttributes::ERROR_TYPE, $spans[0]->attributes());
    }

    public function test_fetch_creates_span_with_row_count(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(
            create()
                ->temporaryTable('test_fetch_span')
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_text()))
                ->constraint(primary_key('id')),
        );
        $client->execute(
            insert()->into('test_fetch_span')->columns('name')->values(param(1))->values(param(2))->values(param(3)),
            ['John', 'Jane', 'Bob'],
        );
        $result = $client->fetchAll(select(star())->from(table('test_fetch_span')));

        static::assertCount(3, $result);

        $spans = $spanProcessor->endedSpans();
        $selectSpan = \array_filter($spans, static fn($s) => \str_contains($s->name(), 'SELECT'));
        $selectSpan = \array_values($selectSpan)[0];

        static::assertSame(3, $selectSpan->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_logging_records_query_execution(): void
    {
        $logProcessor = memory_log_processor(void_exporter());
        $config = $this->createConfig(
            logProcessor: $logProcessor,
            options: postgresql_telemetry_options(logQueries: true),
        );
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(
            create()
                ->temporaryTable('test_logging')
                ->column(column('id', column_type_serial()))
                ->constraint(primary_key('id')),
        );
        $client->execute(insert()->into('test_logging')->defaultValues());

        $logs = $logProcessor->entries();
        static::assertCount(2, $logs);
        static::assertSame('Executing query', $logs[0]->record->body);
        static::assertSame(Severity::DEBUG, $logs[0]->record->severity);
    }

    public function test_metrics_record_operation_duration(): void
    {
        $metricProcessor = memory_metric_processor(void_exporter());
        $config = $this->createConfig(
            metricProcessor: $metricProcessor,
            options: postgresql_telemetry_options(collectMetrics: true),
        );
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(select(literal(1)));

        $this->collectMetrics($config);

        $durationMetrics = $metricProcessor->metricsWithName('operation_duration');
        static::assertCount(1, $durationMetrics);
        static::assertSame(MetricType::HISTOGRAM, $durationMetrics[0]->type);
        static::assertGreaterThan(0, $durationMetrics[0]->value);
    }

    public function test_nested_transaction_creates_savepoint_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
            traceQueries: false,
        ));
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(
            create()
                ->temporaryTable('test_nested_tx')
                ->column(column('id', column_type_serial()))
                ->constraint(primary_key('id')),
        );
        $client->transaction(static function (Client $outer): void {
            $outer->execute(insert()->into('test_nested_tx')->defaultValues());

            $outer->transaction(static function (Client $inner): void {
                $inner->execute(insert()->into('test_nested_tx')->defaultValues());
            });
        });

        $spans = $spanProcessor->endedSpans();
        static::assertCount(2, $spans);

        $spanNames = \array_map(static fn($s) => $s->name(), $spans);
        static::assertContains('BEGIN TRANSACTION', $spanNames);
        static::assertContains('BEGIN SAVEPOINT', $spanNames);
    }

    public function test_parameters_are_included_when_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(includeParameters: true));
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(
            create()
                ->temporaryTable('test_params')
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_text()))
                ->constraint(primary_key('id')),
        );
        $client->execute(insert()->into('test_params')->columns('name')->values(param(1)), ['John']);

        $spans = $spanProcessor->endedSpans();
        $insertSpan = \array_filter($spans, static fn($s) => \str_contains($s->name(), 'INSERT'));
        $insertSpan = \array_values($insertSpan)[0];

        static::assertSame(
            'John',
            $insertSpan->attributes()[PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . '1'],
        );
    }

    public function test_transaction_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, options: postgresql_telemetry_options(
            traceTransactions: true,
            traceQueries: false,
        ));
        $client = traceable_postgresql_client($this->pgsqlContext()->client(), $config);

        $client->execute(
            create()
                ->temporaryTable('test_tx_span')
                ->column(column('id', column_type_serial()))
                ->constraint(primary_key('id')),
        );
        $client->transaction(static function (Client $c): void {
            $c->execute(insert()->into('test_tx_span')->defaultValues());
        });

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('BEGIN TRANSACTION', $spans[0]->name());
        static::assertSame(1, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL]);
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
}
