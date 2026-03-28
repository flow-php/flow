<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use function Flow\PostgreSql\DSL\{pgsql_connection_params, postgresql_telemetry_config, postgresql_telemetry_options};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\PostgreSql\Client\{ConnectionParameters, Cursor};
use Flow\PostgreSql\Client\Telemetry\{PostgreSqlTelemetryAttributes, PostgreSqlTelemetryConfig, PostgreSqlTelemetryOptions, TraceableCursor};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\TestCase;

final class TraceableCursorTest extends TestCase
{
    public function test_count_delegates_to_underlying_cursor() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockCursor = $this->createMock(Cursor::class);
        $mockCursor->method('count')->willReturn(10);

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');

        self::assertSame(10, $cursor->count());
    }

    public function test_free_completes_span_with_row_count() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockCursor = $this->createMock(Cursor::class);
        $mockCursor->method('next')
            ->willReturnOnConsecutiveCalls(
                ['id' => 1],
                ['id' => 2],
                null
            );

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');

        $cursor->next();
        $cursor->next();
        $cursor->next();
        $cursor->free();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SELECT users (cursor)', $spans[0]->name());
        self::assertSame(2, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_free_rethrows_exception_and_records_error() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $exception = new \RuntimeException('Free failed');
        $mockCursor = $this->createMock(Cursor::class);
        $mockCursor->method('free')->willThrowException($exception);

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Free failed');

        try {
            $cursor->free();
        } finally {
            $spans = $spanProcessor->endedSpans();
            self::assertCount(1, $spans);
            self::assertNotNull($spans[0]->status());
            self::assertTrue($spans[0]->status()->isError());
        }
    }

    public function test_iterate_completes_span_after_full_iteration() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockCursor = $this->createMock(Cursor::class);
        $mockCursor->method('iterate')->willReturnCallback(static function () : \Generator {
            yield ['id' => 1];
            yield ['id' => 2];
            yield ['id' => 3];
        });

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');

        $rows = [];

        foreach ($cursor->iterate() as $row) {
            $rows[] = $row;
        }

        self::assertCount(3, $rows);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(3, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_iterate_records_error_on_exception() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockCursor = $this->createMock(Cursor::class);
        $mockCursor->method('iterate')->willReturnCallback(static function () : \Generator {
            yield ['id' => 1];

            throw new \RuntimeException('Iteration failed');
        });

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');

        $this->expectException(\RuntimeException::class);

        try {
            foreach ($cursor->iterate() as $row) {
            }
        } finally {
            $spans = $spanProcessor->endedSpans();
            self::assertCount(1, $spans);
            self::assertNotNull($spans[0]->status());
            self::assertTrue($spans[0]->status()->isError());
            self::assertSame(1, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
        }
    }

    public function test_map_completes_span_after_full_iteration() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockCursor = $this->createMock(Cursor::class);
        $mockCursor->method('map')->willReturnCallback(static function () : \Generator {
            yield new \stdClass();
            yield new \stdClass();
        });

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');

        $objects = [];

        foreach ($cursor->map(new \Flow\PostgreSql\Client\RowMapper\ConstructorMapper(\stdClass::class)) as $object) {
            $objects[] = $object;
        }

        self::assertCount(2, $objects);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(2, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_next_increments_row_count() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockCursor = $this->createMock(Cursor::class);
        $mockCursor->method('next')
            ->willReturnOnConsecutiveCalls(
                ['id' => 1],
                ['id' => 2],
                null
            );

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');

        self::assertSame(['id' => 1], $cursor->next());
        self::assertSame(['id' => 2], $cursor->next());
        self::assertNull($cursor->next());

        $cursor->free();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame(2, $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS]);
    }

    public function test_parameter_count_is_limited_by_default() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            includeParameters: true,
        ));

        $mockCursor = $this->createMock(Cursor::class);

        $parameters = \array_fill(0, 20, 'value');
        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users', $parameters);
        $cursor->free();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        for ($i = 1; $i <= 10; $i++) {
            self::assertArrayHasKey(PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . $i, $spans[0]->attributes());
        }

        for ($i = 11; $i <= 20; $i++) {
            self::assertArrayNotHasKey(PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . $i, $spans[0]->attributes());
        }
    }

    public function test_parameter_count_unlimited_when_null() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            includeParameters: true,
            maxParameters: null,
        ));

        $mockCursor = $this->createMock(Cursor::class);

        $parameters = \array_fill(0, 20, 'value');
        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users', $parameters);
        $cursor->free();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        for ($i = 1; $i <= 20; $i++) {
            self::assertArrayHasKey(PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . $i, $spans[0]->attributes());
        }
    }

    public function test_parameter_values_are_truncated_by_default() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            includeParameters: true,
        ));

        $mockCursor = $this->createMock(Cursor::class);

        $longValue = \str_repeat('a', 200);
        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users WHERE name = $1', [$longValue]);
        $cursor->free();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $paramValue = $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . '1'];
        self::assertSame(103, \strlen($paramValue));
        self::assertStringEndsWith('...', $paramValue);
    }

    public function test_parameter_values_unlimited_when_null() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            includeParameters: true,
            maxParameterLength: null,
        ));

        $mockCursor = $this->createMock(Cursor::class);

        $longValue = \str_repeat('a', 200);
        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users WHERE name = $1', [$longValue]);
        $cursor->free();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $paramValue = $spans[0]->attributes()[PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . '1'];
        self::assertSame(200, \strlen($paramValue));
        self::assertSame($longValue, $paramValue);
    }

    public function test_span_includes_correct_attributes() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);

        $mockCursor = $this->createMock(Cursor::class);

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users WHERE active = $1', [true]);
        $cursor->free();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('SELECT users (cursor)', $span->name());
        self::assertSame('postgresql', $span->attributes()[PostgreSqlTelemetryAttributes::DB_SYSTEM_NAME]);
        self::assertSame('testdb', $span->attributes()[PostgreSqlTelemetryAttributes::DB_NAMESPACE]);
        self::assertSame('localhost', $span->attributes()[PostgreSqlTelemetryAttributes::SERVER_ADDRESS]);
        self::assertSame('SELECT', $span->attributes()[PostgreSqlTelemetryAttributes::DB_OPERATION_NAME]);
        self::assertSame('users', $span->attributes()[PostgreSqlTelemetryAttributes::DB_COLLECTION_NAME]);
    }

    public function test_tracing_disabled_does_not_create_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, postgresql_telemetry_options(
            traceQueries: false,
            collectMetrics: false,
        ));

        $mockCursor = $this->createMock(Cursor::class);

        $cursor = new TraceableCursor($mockCursor, $config, $this->connectionParams(), 'SELECT * FROM users');
        $cursor->free();

        self::assertEmpty($spanProcessor->endedSpans());
    }

    private function connectionParams() : ConnectionParameters
    {
        return pgsql_connection_params('testdb', 'localhost', 5432, 'user');
    }

    private function createConfig(MemorySpanProcessor $spanProcessor, ?PostgreSqlTelemetryOptions $options = null) : PostgreSqlTelemetryConfig
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider(memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );

        return postgresql_telemetry_config($tel, $clock, $options ?? postgresql_telemetry_options());
    }
}
