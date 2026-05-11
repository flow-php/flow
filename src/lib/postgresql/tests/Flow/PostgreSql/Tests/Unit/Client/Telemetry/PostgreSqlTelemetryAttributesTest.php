<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryAttributes;
use PHPUnit\Framework\TestCase;

final class PostgreSqlTelemetryAttributesTest extends TestCase
{
    public function test_db_collection_name_follows_otel_convention(): void
    {
        static::assertSame('db.collection.name', PostgreSqlTelemetryAttributes::DB_COLLECTION_NAME);
    }

    public function test_db_namespace_follows_otel_convention(): void
    {
        static::assertSame('db.namespace', PostgreSqlTelemetryAttributes::DB_NAMESPACE);
    }

    public function test_db_operation_name_follows_otel_convention(): void
    {
        static::assertSame('db.operation.name', PostgreSqlTelemetryAttributes::DB_OPERATION_NAME);
    }

    public function test_db_query_parameter_prefix_follows_otel_convention(): void
    {
        static::assertSame('db.query.parameter.', PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX);
    }

    public function test_db_query_text_follows_otel_convention(): void
    {
        static::assertSame('db.query.text', PostgreSqlTelemetryAttributes::DB_QUERY_TEXT);
    }

    public function test_db_response_returned_rows_follows_otel_convention(): void
    {
        static::assertSame('db.response.returned_rows', PostgreSqlTelemetryAttributes::DB_RESPONSE_RETURNED_ROWS);
    }

    public function test_db_response_status_code_follows_otel_convention(): void
    {
        static::assertSame('db.response.status_code', PostgreSqlTelemetryAttributes::DB_RESPONSE_STATUS_CODE);
    }

    public function test_db_system_name_follows_otel_convention(): void
    {
        static::assertSame('db.system.name', PostgreSqlTelemetryAttributes::DB_SYSTEM_NAME);
        static::assertSame('postgresql', PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL);
    }

    public function test_error_type_follows_otel_convention(): void
    {
        static::assertSame('error.type', PostgreSqlTelemetryAttributes::ERROR_TYPE);
    }

    public function test_server_address_follows_otel_convention(): void
    {
        static::assertSame('server.address', PostgreSqlTelemetryAttributes::SERVER_ADDRESS);
    }

    public function test_server_port_follows_otel_convention(): void
    {
        static::assertSame('server.port', PostgreSqlTelemetryAttributes::SERVER_PORT);
    }

    public function test_transaction_attributes_are_defined(): void
    {
        static::assertSame('db.transaction.nesting_level', PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL);
        static::assertSame('db.transaction.savepoint', PostgreSqlTelemetryAttributes::DB_TRANSACTION_SAVEPOINT);
    }
}
