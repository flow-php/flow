<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Exception;

use Flow\PostgreSql\Client\Exception\{PostgreSqlError, PostgreSqlErrorCategory};
use PHPUnit\Framework\TestCase;

final class PostgreSqlErrorTest extends TestCase
{
    public function test_from_diagnostics_creates_error_with_all_fields() : void
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
            15
        );

        self::assertSame('23505', $error->sqlState);
        self::assertSame(PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION, $error->category);
        self::assertSame('duplicate key value violates unique constraint "users_email_key"', $error->message);
        self::assertSame('Key (email)=(test@example.com) already exists.', $error->detail);
        self::assertSame('Check if the email already exists before inserting.', $error->hint);
        self::assertSame('public', $error->schema);
        self::assertSame('users', $error->table);
        self::assertSame('email', $error->column);
        self::assertSame('users_email_key', $error->constraint);
        self::assertSame(15, $error->position);
    }

    public function test_from_diagnostics_with_minimal_fields() : void
    {
        $error = PostgreSqlError::fromDiagnostics(
            '42601',
            'syntax error at or near "FORM"'
        );

        self::assertSame('42601', $error->sqlState);
        self::assertSame(PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, $error->category);
        self::assertSame('syntax error at or near "FORM"', $error->message);
        self::assertNull($error->detail);
        self::assertNull($error->hint);
        self::assertNull($error->schema);
        self::assertNull($error->table);
        self::assertNull($error->column);
        self::assertNull($error->constraint);
        self::assertNull($error->position);
    }

    public function test_full_message_returns_original_message() : void
    {
        $originalMessage = 'duplicate key value violates unique constraint "users_email_key"';
        $error = PostgreSqlError::fromDiagnostics('23505', $originalMessage);

        self::assertSame($originalMessage, $error->fullMessage());
    }

    public function test_is_check_violation() : void
    {
        $error = PostgreSqlError::fromDiagnostics('23514', 'check violation');

        self::assertTrue($error->isCheckViolation());
        self::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_connection_error() : void
    {
        $error = PostgreSqlError::fromDiagnostics('08006', 'connection failure');

        self::assertTrue($error->isConnectionError());
    }

    public function test_is_data_error() : void
    {
        $error = PostgreSqlError::fromDiagnostics('22001', 'string data right truncation');

        self::assertTrue($error->isDataError());
    }

    public function test_is_deadlock_detected() : void
    {
        $error = PostgreSqlError::fromDiagnostics('40P01', 'deadlock detected');

        self::assertTrue($error->isDeadlockDetected());
        self::assertTrue($error->isTransactionRollback());
    }

    public function test_is_exclusion_violation() : void
    {
        $error = PostgreSqlError::fromDiagnostics('23P01', 'exclusion violation');

        self::assertTrue($error->isExclusionViolation());
        self::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_foreign_key_violation() : void
    {
        $error = PostgreSqlError::fromDiagnostics('23503', 'foreign key violation');

        self::assertTrue($error->isForeignKeyViolation());
        self::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_not_null_violation() : void
    {
        $error = PostgreSqlError::fromDiagnostics('23502', 'not null violation');

        self::assertTrue($error->isNotNullViolation());
        self::assertTrue($error->isIntegrityViolation());
    }

    public function test_is_serialization_failure() : void
    {
        $error = PostgreSqlError::fromDiagnostics('40001', 'serialization failure');

        self::assertTrue($error->isSerializationFailure());
        self::assertTrue($error->isTransactionRollback());
    }

    public function test_is_syntax_error() : void
    {
        $error = PostgreSqlError::fromDiagnostics('42601', 'syntax error');

        self::assertTrue($error->isSyntaxError());
        self::assertFalse($error->isIntegrityViolation());
    }

    public function test_is_transaction_rollback() : void
    {
        $error = PostgreSqlError::fromDiagnostics('40000', 'transaction rollback');

        self::assertTrue($error->isTransactionRollback());
    }

    public function test_is_unique_violation() : void
    {
        $error = PostgreSqlError::fromDiagnostics('23505', 'unique violation');

        self::assertTrue($error->isUniqueViolation());
        self::assertTrue($error->isIntegrityViolation());
    }

    public function test_safe_message_hides_sensitive_details() : void
    {
        $error = PostgreSqlError::fromDiagnostics(
            '23505',
            'duplicate key value violates unique constraint "users_email_key"',
            'Key (email)=(secret@company.com) already exists.',
            null,
            'internal_schema',
            'secret_users_table',
            'email',
            'users_email_key'
        );

        $safeMessage = $error->safeMessage();

        self::assertStringNotContainsString('secret@company.com', $safeMessage);
        self::assertStringNotContainsString('internal_schema', $safeMessage);
        self::assertStringNotContainsString('secret_users_table', $safeMessage);
        self::assertStringNotContainsString('users_email_key', $safeMessage);
        self::assertSame('Data constraint violation', $safeMessage);
    }

    public function test_safe_message_returns_category_safe_message() : void
    {
        $error = PostgreSqlError::fromDiagnostics(
            '23505',
            'duplicate key value violates unique constraint "users_email_key"',
            table: 'users',
            constraint: 'users_email_key'
        );

        self::assertSame('Data constraint violation', $error->safeMessage());
    }

    public function test_unknown_creates_generic_error() : void
    {
        $error = PostgreSqlError::unknown();

        self::assertSame('00000', $error->sqlState);
        self::assertSame(PostgreSqlErrorCategory::UNKNOWN, $error->category);
        self::assertSame('Unknown error', $error->message);
    }

    public function test_unknown_with_custom_message() : void
    {
        $error = PostgreSqlError::unknown('Connection lost');

        self::assertSame('00000', $error->sqlState);
        self::assertSame(PostgreSqlErrorCategory::UNKNOWN, $error->category);
        self::assertSame('Connection lost', $error->message);
    }
}
