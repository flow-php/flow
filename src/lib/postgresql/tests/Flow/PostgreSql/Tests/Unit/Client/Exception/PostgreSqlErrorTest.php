<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\PostgreSqlError;
use Flow\PostgreSql\Client\Exception\PostgreSqlErrorCategory;
use PHPUnit\Framework\TestCase;

final class PostgreSqlErrorTest extends TestCase
{
    public function test_from_diagnostics_creates_error_with_all_fields(): void
    {
        $error = PostgreSqlError::fromDiagnostics(
            '23505',
            'duplicate key value violates unique constraint "users_email_key"',
            'Key (email)=(test@example.com) already exists.',
            'Check if the email already exists before inserting.',
            'public',
            'users',
            'email',
            'users_email_key',
            15,
        );

        static::assertSame('23505', $error->sqlState);
        static::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $error->category);
        static::assertSame('duplicate key value violates unique constraint "users_email_key"', $error->message);
        static::assertSame('Key (email)=(test@example.com) already exists.', $error->detail);
        static::assertSame('Check if the email already exists before inserting.', $error->hint);
        static::assertSame('public', $error->schema);
        static::assertSame('users', $error->table);
        static::assertSame('email', $error->column);
        static::assertSame('users_email_key', $error->constraint);
        static::assertSame(15, $error->position);
    }

    public function test_from_diagnostics_with_minimal_fields(): void
    {
        $error = PostgreSqlError::fromDiagnostics('42601', 'syntax error at or near "FORM"');

        static::assertSame('42601', $error->sqlState);
        static::assertSame(PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, $error->category);
        static::assertSame('syntax error at or near "FORM"', $error->message);
        static::assertNull($error->detail);
        static::assertNull($error->hint);
        static::assertNull($error->schema);
        static::assertNull($error->table);
        static::assertNull($error->column);
        static::assertNull($error->constraint);
        static::assertNull($error->position);
    }

    public function test_full_message_returns_original_message(): void
    {
        $originalMessage = 'duplicate key value violates unique constraint "users_email_key"';
        $error = PostgreSqlError::fromDiagnostics('23505', $originalMessage);

        static::assertSame($originalMessage, $error->fullMessage());
    }

    public function test_is_check_violation(): void
    {
        $error = PostgreSqlError::fromDiagnostics('23514', 'check violation');

        static::assertTrue($error->isCheckViolation());
        static::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_connection_error(): void
    {
        $error = PostgreSqlError::fromDiagnostics('08006', 'connection failure');

        static::assertTrue($error->isConnectionError());
    }

    public function test_is_data_error(): void
    {
        $error = PostgreSqlError::fromDiagnostics('22001', 'string data right truncation');

        static::assertTrue($error->isDataError());
    }

    public function test_is_deadlock_detected(): void
    {
        $error = PostgreSqlError::fromDiagnostics('40P01', 'deadlock detected');

        static::assertTrue($error->isDeadlockDetected());
        static::assertTrue($error->isTransactionRollback());
    }

    public function test_is_exclusion_violation(): void
    {
        $error = PostgreSqlError::fromDiagnostics('23P01', 'exclusion violation');

        static::assertTrue($error->isExclusionViolation());
        static::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_foreign_key_violation(): void
    {
        $error = PostgreSqlError::fromDiagnostics('23503', 'foreign key violation');

        static::assertTrue($error->isForeignKeyViolation());
        static::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_not_null_violation(): void
    {
        $error = PostgreSqlError::fromDiagnostics('23502', 'not null violation');

        static::assertTrue($error->isNotNullViolation());
        static::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_serialization_failure(): void
    {
        $error = PostgreSqlError::fromDiagnostics('40001', 'serialization failure');

        static::assertTrue($error->isSerializationFailure());
        static::assertTrue($error->isTransactionRollback());
    }

    public function test_is_syntax_error(): void
    {
        $error = PostgreSqlError::fromDiagnostics('42601', 'syntax error');

        static::assertTrue($error->isSyntaxError());
        static::assertFalse($error->isIntegrityViolation());
    }

    public function test_is_transaction_rollback(): void
    {
        $error = PostgreSqlError::fromDiagnostics('40000', 'transaction rollback');

        static::assertTrue($error->isTransactionRollback());
    }

    public function test_is_unique_violation(): void
    {
        $error = PostgreSqlError::fromDiagnostics('23505', 'unique violation');

        static::assertTrue($error->isUniqueViolation());
        static::assertTrue($error->isIntegrityViolation());
    }

    public function test_safe_message_hides_sensitive_details(): void
    {
        $error = PostgreSqlError::fromDiagnostics(
            '23505',
            'duplicate key value violates unique constraint "users_email_key"',
            'Key (email)=(secret@company.com) already exists.',
            null,
            'internal_schema',
            'secret_users_table',
            'email',
            'users_email_key',
        );

        $safeMessage = $error->safeMessage();

        static::assertStringNotContainsString('secret@company.com', $safeMessage);
        static::assertStringNotContainsString('internal_schema', $safeMessage);
        static::assertStringNotContainsString('secret_users_table', $safeMessage);
        static::assertStringNotContainsString('users_email_key', $safeMessage);
        static::assertSame('Data constraint violation', $safeMessage);
    }

    public function test_safe_message_returns_category_safe_message(): void
    {
        $error = PostgreSqlError::fromDiagnostics(
            '23505',
            'duplicate key value violates unique constraint "users_email_key"',
            table: 'users',
            constraint: 'users_email_key',
        );

        static::assertSame('Data constraint violation', $error->safeMessage());
    }

    public function test_unknown_creates_generic_error(): void
    {
        $error = PostgreSqlError::unknown();

        static::assertSame('00000', $error->sqlState);
        static::assertSame(PostgreSqlErrorCategory::UNKNOWN, $error->category);
        static::assertSame('Unknown error', $error->message);
    }

    public function test_unknown_with_custom_message(): void
    {
        $error = PostgreSqlError::unknown('Connection lost');

        static::assertSame('00000', $error->sqlState);
        static::assertSame(PostgreSqlErrorCategory::UNKNOWN, $error->category);
        static::assertSame('Connection lost', $error->message);
    }
}
