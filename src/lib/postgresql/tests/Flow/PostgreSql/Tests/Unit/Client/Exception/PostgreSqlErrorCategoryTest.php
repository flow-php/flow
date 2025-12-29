<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\PostgreSqlErrorCategory;
use PHPUnit\Framework\TestCase;

final class PostgreSqlErrorCategoryTest extends TestCase
{
    public function test_from_sql_state_connection_exception() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('08006');

        self::assertSame(PostgreSqlErrorCategory::CONNECTION_EXCEPTION, $category);
    }

    public function test_from_sql_state_data_exception() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('22001');

        self::assertSame(PostgreSqlErrorCategory::DATA_EXCEPTION, $category);
    }

    public function test_from_sql_state_empty_returns_unknown() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('');

        self::assertSame(PostgreSqlErrorCategory::UNKNOWN, $category);
    }

    public function test_from_sql_state_integrity_constraint_violation() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('23505');

        self::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $category);
    }

    public function test_from_sql_state_short_returns_unknown() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('2');

        self::assertSame(PostgreSqlErrorCategory::UNKNOWN, $category);
    }

    public function test_from_sql_state_syntax_error() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('42601');

        self::assertSame(PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, $category);
    }

    public function test_from_sql_state_transaction_rollback() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('40001');

        self::assertSame(PostgreSqlErrorCategory::TRANSACTION_ROLLBACK, $category);
    }

    public function test_from_sql_state_unknown_returns_unknown() : void
    {
        $category = PostgreSqlErrorCategory::fromSqlState('99999');

        self::assertSame(PostgreSqlErrorCategory::UNKNOWN, $category);
    }

    public function test_integrity_constraint_violation_is_not_recoverable() : void
    {
        self::assertFalse(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION->isRecoverable());
    }

    public function test_safe_message_for_connection_exception() : void
    {
        $message = PostgreSqlErrorCategory::CONNECTION_EXCEPTION->safeMessage();

        self::assertSame('Database connection error occurred', $message);
    }

    public function test_safe_message_for_data_exception() : void
    {
        $message = PostgreSqlErrorCategory::DATA_EXCEPTION->safeMessage();

        self::assertSame('Invalid data format or value', $message);
    }

    public function test_safe_message_for_integrity_constraint() : void
    {
        $message = PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION->safeMessage();

        self::assertSame('Data constraint violation', $message);
    }

    public function test_safe_message_for_syntax_error() : void
    {
        $message = PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION->safeMessage();

        self::assertSame('Query syntax or permission error', $message);
    }

    public function test_safe_message_for_unknown() : void
    {
        $message = PostgreSqlErrorCategory::UNKNOWN->safeMessage();

        self::assertSame('Database operation failed', $message);
    }

    public function test_transaction_rollback_is_recoverable() : void
    {
        self::assertTrue(PostgreSqlErrorCategory::TRANSACTION_ROLLBACK->isRecoverable());
    }
}
