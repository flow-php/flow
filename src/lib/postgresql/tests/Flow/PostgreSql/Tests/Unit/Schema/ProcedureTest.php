<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\schema_procedure;

use PHPUnit\Framework\TestCase;

final class ProcedureTest extends TestCase
{
    public function test_procedure_construction() : void
    {
        $proc = schema_procedure('cleanup_old_records');

        self::assertSame('cleanup_old_records', $proc->name);
        self::assertSame([], $proc->argumentTypes);
        self::assertSame('sql', $proc->language);
        self::assertNull($proc->definition);
    }

    public function test_procedure_with_arguments() : void
    {
        $proc = schema_procedure(
            'archive_user',
            argumentTypes: ['integer', 'text'],
            language: 'plpgsql',
            definition: 'BEGIN DELETE FROM users WHERE id = $1; END;',
        );

        self::assertSame(['integer', 'text'], $proc->argumentTypes);
        self::assertSame('plpgsql', $proc->language);
        self::assertSame('BEGIN DELETE FROM users WHERE id = $1; END;', $proc->definition);
    }

    public function test_to_sql_generates_create_procedure() : void
    {
        self::assertSame(
            'CREATE OR REPLACE PROCEDURE archive_user(IN int, IN text) LANGUAGE plpgsql AS $$BEGIN DELETE FROM users WHERE id = $1; END;$$',
            schema_procedure(
                'archive_user',
                argumentTypes: ['integer', 'text'],
                language: 'plpgsql',
                definition: 'BEGIN DELETE FROM users WHERE id = $1; END;',
            )->toSql()->toSql(),
        );
    }

    public function test_to_sql_returns_null_when_definition_null() : void
    {
        self::assertNull(
            schema_procedure('cleanup_old_records')->toSql(),
        );
    }
}
