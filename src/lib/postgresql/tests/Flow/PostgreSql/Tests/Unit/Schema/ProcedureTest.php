<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\Procedure;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_procedure;

final class ProcedureTest extends TestCase
{
    public function test_schema_round_trips_through_normalize(): void
    {
        $procedure = schema_procedure('p', ['integer'], 'sql', 'SELECT 1', 'pivotly');

        static::assertEquals($procedure, Procedure::fromArray($procedure->normalize()));
    }

    public function test_with_schema_qualifies_name_and_keeps_every_other_field(): void
    {
        $procedure = schema_procedure('p', ['integer'], 'sql', 'SELECT 1');

        static::assertSame('public.p', $procedure->qualifiedName());
        static::assertEquals(
            schema_procedure('p', ['integer'], 'sql', 'SELECT 1', 'pivotly'),
            $procedure->withSchema('pivotly'),
        );
        static::assertSame('pivotly.p', $procedure->withSchema('pivotly')->qualifiedName());
    }

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
        $sql = schema_procedure(
            'archive_user',
            argumentTypes: ['integer', 'text'],
            language: 'plpgsql',
            definition: 'BEGIN DELETE FROM users WHERE id = $1; END;',
        )->toSql();

        static::assertNotNull($sql);
        static::assertSame(
            'CREATE OR REPLACE PROCEDURE public.archive_user(IN int, IN text) LANGUAGE plpgsql AS $$BEGIN DELETE FROM users WHERE id = $1; END;$$',
            $sql->toSql(),
        );
    }

    public function test_to_sql_returns_null_when_definition_null(): void
    {
        static::assertNull(schema_procedure('cleanup_old_records')->toSql());
    }
}
