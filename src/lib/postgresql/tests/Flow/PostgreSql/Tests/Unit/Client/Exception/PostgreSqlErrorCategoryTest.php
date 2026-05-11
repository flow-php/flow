<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\PostgreSqlErrorCategory;
use PHPUnit\Framework\TestCase;

final class PostgreSqlErrorCategoryTest extends TestCase
{
    public function test_from_sql_state_connection_exception(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('08006');

        static::assertSame(PostgreSqlErrorCategory::CONNECTION_EXCEPTION, $category);
    }

    public function test_from_sql_state_data_exception(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('22001');

        static::assertSame(PostgreSqlErrorCategory::DATA_EXCEPTION, $category);
    }

    public function test_from_sql_state_empty_returns_unknown(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('');

        static::assertSame(PostgreSqlErrorCategory::UNKNOWN, $category);
    }

    public function test_from_sql_state_integrity_constraint_violation(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('23505');

        static::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $category);
    }

    public function test_from_sql_state_short_returns_unknown(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('2');

        static::assertSame(PostgreSqlErrorCategory::UNKNOWN, $category);
    }

    public function test_from_sql_state_syntax_error(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('42601');

        static::assertSame(PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, $category);
    }

    public function test_from_sql_state_transaction_rollback(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('40001');

        static::assertSame(PostgreSqlErrorCategory::TRANSACTION_ROLLBACK, $category);
    }

    public function test_from_sql_state_unknown_returns_unknown(): void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('99999');

        static::assertSame(PostgreSqlErrorCategory::UNKNOWN, $category);
    }

    public function test_integrity_constraint_violation_is_not_recoverable(): void
    {
        static::assertFalse(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION->isRecoverable());
    }

    public function test_safe_message_for_connection_exception(): void
    {
        $message = PostgreSqlErrorCategory::CONNECTION_EXCEPTION->safeMessage();

        static::assertSame('Database connection error occurred', $message);
    }

    public function test_safe_message_for_data_exception(): void
    {
        $message = PostgreSqlErrorCategory::DATA_EXCEPTION->safeMessage();

        static::assertSame('Invalid data format or value', $message);
    }

    public function test_safe_message_for_integrity_constraint(): void
    {
        $message = PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION->safeMessage();

        static::assertSame('Data constraint violation', $message);
    }

    public function test_safe_message_for_syntax_error(): void
    {
        $message = PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION->safeMessage();

        static::assertSame('Query syntax or permission error', $message);
    }

    public function test_safe_message_for_unknown(): void
    {
        $message = PostgreSqlErrorCategory::UNKNOWN->safeMessage();

        static::assertSame('Database operation failed', $message);
    }

    public function test_transaction_rollback_is_recoverable(): void
    {
        static::assertTrue(PostgreSqlErrorCategory::TRANSACTION_ROLLBACK->isRecoverable());
    }
}
