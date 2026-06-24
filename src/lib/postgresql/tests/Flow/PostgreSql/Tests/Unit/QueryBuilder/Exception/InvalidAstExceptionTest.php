<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Exception;

use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use PHPUnit\Framework\TestCase;

final class InvalidAstExceptionTest extends TestCase
{
    public function test_invalid_field_value(): void
    {
        static::assertSame(
            'Invalid value for field "stmts" in ParseResult node: expected at least one statement',
            InvalidAstException::invalidFieldValue(
                'stmts',
                'ParseResult',
                'expected at least one statement',
            )->getMessage(),
        );
    }

    public function test_missing_required_field(): void
    {
        static::assertSame(
            'Missing required field "val" in ResTarget node',
            InvalidAstException::missingRequiredField('val', 'ResTarget')->getMessage(),
        );
    }

    public function test_unexpected_node_type_with_version(): void
    {
        $message = InvalidAstException::unexpectedNodeType('SelectStmt', 'update_stmt', 170007)->getMessage();

        static::assertStringContainsString('Expected SelectStmt node', $message);
        static::assertStringContainsString('"update_stmt"', $message);
        static::assertStringContainsString('170007', $message);
        static::assertStringContainsString('different PostgreSQL major', $message);
    }

    public function test_unexpected_node_type_without_version(): void
    {
        static::assertSame(
            'Expected SelectStmt node, got unknown',
            InvalidAstException::unexpectedNodeType('SelectStmt', 'unknown')->getMessage(),
        );
    }
}
