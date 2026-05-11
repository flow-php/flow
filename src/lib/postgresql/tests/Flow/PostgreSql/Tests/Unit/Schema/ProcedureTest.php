<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_procedure;

final class ProcedureTest extends TestCase
{
    public function test_procedure_construction(): void
    {
        $proc = schema_procedure('cleanup_old_records');

        static::assertSame('cleanup_old_records', $proc->name);
        static::assertSame([], $proc->argumentTypes);
        static::assertSame('sql', $proc->language);
        static::assertNull($proc->definition);
    }

    public function test_procedure_with_arguments(): void
    {
        $proc = schema_procedure(
            'archive_user',
            argumentTypes: ['integer', 'text'],
            language: 'plpgsql',
            definition: 'BEGIN DELETE FROM users WHERE id = $1; END;',
        );

        static::assertSame(['integer', 'text'], $proc->argumentTypes);
        static::assertSame('plpgsql', $proc->language);
        static::assertSame('BEGIN DELETE FROM users WHERE id = $1; END;', $proc->definition);
    }

    public function test_to_sql_generates_create_procedure(): void
    {
        static::assertSame(
            'CREATE OR REPLACE PROCEDURE archive_user(IN int, IN text) LANGUAGE plpgsql AS $$BEGIN DELETE FROM users WHERE id = $1; END;$$',
            schema_procedure(
                'archive_user',
                argumentTypes: ['integer', 'text'],
                language: 'plpgsql',
                definition: 'BEGIN DELETE FROM users WHERE id = $1; END;',
            )
                ->toSql()
                ->toSql(),
        );
    }

    public function test_to_sql_returns_null_when_definition_null(): void
    {
        static::assertNull(schema_procedure('cleanup_old_records')->toSql());
    }
}
