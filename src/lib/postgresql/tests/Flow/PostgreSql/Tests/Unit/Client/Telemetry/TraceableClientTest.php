<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use Flow\PostgreSql\Client\Cursor;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryConfig;
use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryOptions;
use Flow\PostgreSql\Client\Telemetry\TraceableCursor;
use Flow\PostgreSql\Client\Telemetry\TransactionSpanMode;
use Flow\PostgreSql\Client\Types\ValueConverters;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\SemConvAttributes;
use PHPUnit\Framework\MockObject\Stub;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function array_fill;
use function Flow\PostgreSql\DSL\column_type_from_string;
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
use function str_repeat;
use function strlen;

final class TraceableClientTest extends TestCase
{
    public function test_all_telemetry_disabled_does_not_wrap_cursor(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            traceQueries: false,
            transactionSpans: TransactionSpanMode::OFF,
            collectMetrics: false,
        ));

        $mockCursor = $this->createStub(Cursor::class);
        $mockClient = $this->createMockClient();
        $mockClient->method('cursor')->willReturn($mockCursor);

        $client = traceable_postgresql_client($mockClient, $config);
        $cursor = $client->cursor('SELECT * FROM users');

        static::assertSame($mockCursor, $cursor);
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_close_delegates_to_underlying_client(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMock(Client::class);
        $mockClient->method('parameters')->willReturn(pgsql_connection_params('testdb', 'localhost', 5432, 'user'));
        $mockClient->expects(self::once())->method('close');

        $client = traceable_postgresql_client($mockClient, $config);
        $client->close();
    }

    public function test_converters_delegates_to_underlying_client(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $converters = ValueConverters::create();
        $mockClient = $this->createMockClient();
        $mockClient->method('converters')->willReturn($converters);

        $client = traceable_postgresql_client($mockClient, $config);

        static::assertSame($converters, $client->converters());
    }

    public function test_cursor_returns_traceable_cursor_when_tracing_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(traceQueries: true));

        $mockCursor = $this->createStub(Cursor::class);
        $mockClient = $this->createMockClient();
        $mockClient->method('cursor')->willReturn($mockCursor);

        $client = traceable_postgresql_client($mockClient, $config);
        $cursor = $client->cursor('SELECT * FROM users');

        static::assertInstanceOf(TraceableCursor::class, $cursor);
    }

    public function test_default_port_is_not_included_in_attributes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET active = true');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertArrayNotHasKey(SemConvAttributes::SERVER_PORT, $spans[0]->attributes());
    }

    public function test_describe_delegates_and_logs_the_probe(): void
    {
        $columns = [['name' => 'id', 'type' => column_type_from_string('int8')]];
        $mockClient = $this->createMockClient();
        $mockClient->method('describe')->willReturn($columns);

        $client = traceable_postgresql_client($mockClient, $this->createConfig(memory_span_processor(void_exporter())));

        static::assertSame($columns, $client->describe('SELECT id FROM users WHERE id > $1', [1]));
    }

    public function test_execute_creates_span_with_correct_attributes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(5);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->execute('UPDATE users SET active = $1 WHERE id = $2', [true, 123]);

        static::assertSame(5, $result);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('UPDATE users', $span->name());
        static::assertSame('postgresql', $span->attributes()[SemConvAttributes::DB_SYSTEM_NAME]);
        static::assertSame('testdb', $span->attributes()[SemConvAttributes::DB_NAMESPACE]);
        static::assertSame('localhost', $span->attributes()[SemConvAttributes::SERVER_ADDRESS]);
        static::assertSame('UPDATE', $span->attributes()[SemConvAttributes::DB_OPERATION_NAME]);
        static::assertSame('users', $span->attributes()[SemConvAttributes::DB_COLLECTION_NAME]);
        static::assertSame(5, $span->attributes()[SemConvAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_execute_rethrows_exception_and_records_error(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $exception = new RuntimeException('Query failed');
        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willThrowException($exception);

        $client = traceable_postgresql_client($mockClient, $config);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Query failed');

        try {
            $client->execute('UPDATE users SET active = $1', [true]);
        } finally {
            $spans = $spanProcessor->endedSpans();
            static::assertCount(1, $spans);
            $status = $spans[0]->status();
            static::assertNotNull($status);
            static::assertTrue($status->isError());
        }
    }

    public function test_commit_rethrows_exception_and_records_error(): void
    {
        $config = $this->createConfig(memory_span_processor(void_exporter()));

        $mockClient = $this->createMockClient();
        $mockClient->method('commit')->willThrowException(new RuntimeException('Commit failed'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Commit failed');

        traceable_postgresql_client($mockClient, $config)->commit();
    }

    public function test_roll_back_rethrows_exception_and_records_error(): void
    {
        $config = $this->createConfig(memory_span_processor(void_exporter()));

        $mockClient = $this->createMockClient();
        $mockClient->method('rollBack')->willThrowException(new RuntimeException('Rollback failed'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Rollback failed');

        traceable_postgresql_client($mockClient, $config)->rollBack();
    }

    public function test_fetch_all_creates_span_with_row_count(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient
            ->method('fetchAll')
            ->willReturn([
                ['id' => 1, 'name' => 'User 1'],
                ['id' => 2, 'name' => 'User 2'],
                ['id' => 3, 'name' => 'User 3'],
            ]);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetchAll('SELECT * FROM users');

        static::assertCount(3, $result);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT users', $spans[0]->name());
        static::assertSame(3, $spans[0]->attributes()[SemConvAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_creates_span_with_row_count(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('fetch')->willReturn(['id' => 1, 'name' => 'Test']);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetch('SELECT * FROM users WHERE id = $1', [1]);

        static::assertSame(['id' => 1, 'name' => 'Test'], $result);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(1, $spans[0]->attributes()[SemConvAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_fetch_null_result_records_zero_rows(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('fetch')->willReturn(null);

        $client = traceable_postgresql_client($mockClient, $config);
        $result = $client->fetch('SELECT * FROM users WHERE id = $1', [999]);

        static::assertNull($result);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(0, $spans[0]->attributes()[SemConvAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_get_transaction_nesting_level_delegates(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('getTransactionNestingLevel')->willReturn(2);

        $client = traceable_postgresql_client($mockClient, $config);

        static::assertSame(2, $client->getTransactionNestingLevel());
    }

    public function test_is_auto_commit_delegates(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('isAutoCommit')->willReturn(true);

        $client = traceable_postgresql_client($mockClient, $config);

        static::assertTrue($client->isAutoCommit());
    }

    public function test_is_connected_delegates(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('isConnected')->willReturn(true);

        $client = traceable_postgresql_client($mockClient, $config);

        static::assertTrue($client->isConnected());
    }

    public function test_last_insert_id_delegates(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMock(Client::class);
        $mockClient->method('parameters')->willReturn(pgsql_connection_params('testdb', 'localhost', 5432, 'user'));
        $mockClient->expects(self::once())->method('lastInsertId')->with('users_id_seq')->willReturn(42);

        $client = traceable_postgresql_client($mockClient, $config);

        static::assertSame(42, $client->lastInsertId('users_id_seq'));
    }

    public function test_non_default_port_is_included_in_attributes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $connectionParams = pgsql_connection_params('testdb', 'localhost', 5433, 'user');
        $mockClient = $this->createMockClient($connectionParams);
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET active = true');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(5433, $spans[0]->attributes()[SemConvAttributes::SERVER_PORT]);
    }

    public function test_parameter_count_is_limited_by_default(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(includeParameters: true));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);

        $parameters = array_fill(0, 20, 'value');
        $client->execute('SELECT * FROM users WHERE id IN ($1, $2, ...)', $parameters);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        for ($i = 1; $i <= 10; $i++) {
            static::assertArrayHasKey(SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . $i, $spans[0]->attributes());
        }

        for ($i = 11; $i <= 20; $i++) {
            static::assertArrayNotHasKey(SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . $i, $spans[0]->attributes());
        }
    }

    public function test_parameter_count_unlimited_when_null(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            includeParameters: true,
            maxParameters: null,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);

        $parameters = array_fill(0, 20, 'value');
        $client->execute('SELECT * FROM users WHERE id IN ($1, $2, ...)', $parameters);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        for ($i = 1; $i <= 20; $i++) {
            static::assertArrayHasKey(SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . $i, $spans[0]->attributes());
        }
    }

    public function test_parameter_limits_ignored_when_include_parameters_false(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            includeParameters: false,
            maxParameters: 5,
            maxParameterLength: 50,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET name = $1 WHERE id = $2', ['John', 123]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertArrayNotHasKey(SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . '1', $spans[0]->attributes());
        static::assertArrayNotHasKey(SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . '2', $spans[0]->attributes());
    }

    public function test_parameter_values_are_truncated_by_default(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(includeParameters: true));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);

        $longValue = str_repeat('a', 200);
        $client->execute('UPDATE users SET name = $1', [$longValue]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $paramValue = $spans[0]->attributes()[SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . '1'];
        static::assertIsString($paramValue);
        static::assertSame(103, strlen($paramValue));
        static::assertStringEndsWith('...', $paramValue);
    }

    public function test_parameter_values_unlimited_when_null(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            includeParameters: true,
            maxParameterLength: null,
        ));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);

        $longValue = str_repeat('a', 200);
        $client->execute('UPDATE users SET name = $1', [$longValue]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $paramValue = $spans[0]->attributes()[SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . '1'];
        static::assertIsString($paramValue);
        static::assertSame(200, strlen($paramValue));
        static::assertSame($longValue, $paramValue);
    }

    public function test_parameters_are_included_when_enabled(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(includeParameters: true));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET name = $1 WHERE id = $2', ['John', 123]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('John', $spans[0]->attributes()[SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . '1']);
        static::assertSame('123', $spans[0]->attributes()[SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . '2']);
    }

    public function test_parameters_are_not_included_by_default(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('UPDATE users SET name = $1 WHERE id = $2', ['John', 123]);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertArrayNotHasKey(SemConvAttributes::DB_QUERY_PARAMETER_PREFIX . '1', $spans[0]->attributes());
    }

    public function test_query_text_is_not_truncated_when_max_length_null(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(maxQueryLength: null));

        $longQuery = 'SELECT * FROM users WHERE ' . str_repeat('id = 1 AND ', 100) . 'active = true';
        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute($longQuery);

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame($longQuery, $spans[0]->attributes()[SemConvAttributes::DB_QUERY_TEXT]);
    }

    public function test_query_text_is_truncated_when_max_length_set(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(maxQueryLength: 20));

        $mockClient = $this->createMockClient();
        $mockClient->method('execute')->willReturn(1);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->execute('SELECT * FROM users WHERE id = 123 AND active = true');

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('SELECT * FROM users ...', $spans[0]->attributes()[SemConvAttributes::DB_QUERY_TEXT]);
    }

    public function test_set_auto_commit_delegates(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockClient = $this->createMock(Client::class);
        $mockClient->method('parameters')->willReturn(pgsql_connection_params('testdb', 'localhost', 5432, 'user'));
        $mockClient->expects(self::once())->method('setAutoCommit')->with(false);

        $client = traceable_postgresql_client($mockClient, $config);
        $client->setAutoCommit(false);
    }

    private function createConfig(
        MemorySpanProcessor $spanProcessor,
        ?PostgreSqlTelemetryOptions $options = null,
    ): PostgreSqlTelemetryConfig {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_exporter()), $clock),
            logger_provider(memory_log_processor(void_exporter()), $clock, $contextStorage),
        );

        return postgresql_telemetry_config($tel, $clock, $options ?? postgresql_telemetry_options());
    }

    /**
     * @return Client&Stub
     */
    private function createMockClient(?ConnectionParameters $connectionParams = null): Client
    {
        $mockClient = $this->createStub(Client::class);
        $mockClient
            ->method('parameters')
            ->willReturn($connectionParams ?? pgsql_connection_params('testdb', 'localhost', 5432, 'user'));

        return $mockClient;
    }
}
