<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\ProcedureDiff;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_procedure;

final class ProcedureDiffTest extends TestCase
{
    public function test_generates_create_or_replace_procedure(): void
    {
        $diff = new ProcedureDiff(
            schema_procedure('cleanup', ['integer'], 'sql', 'DELETE FROM logs WHERE id = $1'),
            schema_procedure('cleanup', ['integer'], 'plpgsql', 'BEGIN DELETE FROM logs WHERE id = $1; END'),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE OR REPLACE PROCEDURE cleanup(IN int) LANGUAGE plpgsql AS $$BEGIN DELETE FROM logs WHERE id = $1; END$$',
            $sqls[0]->toSql(),
        );
    }

    public function test_returns_empty_when_definition_null(): void
    {
        $diff = new ProcedureDiff(
            schema_procedure('cleanup', ['integer'], 'sql', 'DELETE FROM logs WHERE id = $1'),
            schema_procedure('cleanup', ['integer'], 'plpgsql'),
        );

        static::assertSame([], $diff->generate());
    }

    public function test_reversed_procedure_definition_change(): void
    {
        $diff = new ProcedureDiff(
            schema_procedure('cleanup', ['integer'], 'plpgsql', 'BEGIN DELETE FROM logs WHERE id = $1; END'),
            schema_procedure('cleanup', ['integer'], 'sql', 'DELETE FROM logs WHERE id = $1'),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE OR REPLACE PROCEDURE cleanup(IN int) LANGUAGE sql AS $$DELETE FROM logs WHERE id = $1$$',
            $sqls[0]->toSql(),
        );
    }
}
