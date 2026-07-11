<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\PostgreSql\Client\Telemetry\PostgreSqlTelemetryAttributes;
use PHPUnit\Framework\TestCase;

final class PostgreSqlTelemetryAttributesTest extends TestCase
{
    public function test_custom_keys_use_the_flow_db_prefix(): void
    {
        static::assertStringStartsWith('flow.db.', PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL);
        static::assertStringStartsWith('flow.db.', PostgreSqlTelemetryAttributes::DB_TRANSACTION_SAVEPOINT);
    }

    public function test_db_system_value_is_postgresql(): void
    {
        static::assertSame('postgresql', PostgreSqlTelemetryAttributes::DB_SYSTEM_POSTGRESQL);
    }

    public function test_transaction_attributes_are_defined(): void
    {
        static::assertSame(
            'flow.db.transaction.nesting_level',
            PostgreSqlTelemetryAttributes::DB_TRANSACTION_NESTING_LEVEL,
        );
        static::assertSame('flow.db.transaction.savepoint', PostgreSqlTelemetryAttributes::DB_TRANSACTION_SAVEPOINT);
    }
}
