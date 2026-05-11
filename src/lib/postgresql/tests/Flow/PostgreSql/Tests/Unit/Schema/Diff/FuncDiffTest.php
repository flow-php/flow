<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\FuncDiff;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema_function;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class FuncDiffTest extends TestCase
{
    public function test_generates_create_or_replace_function(): void
    {
        $diff = new FuncDiff(
            schema_function(
                'get_user',
                'text',
                ['integer'],
                'sql',
                select(col('name'))
                    ->from(table('users'))
                    ->where(eq(col('id'), literal(1)))
                    ->toSql(),
            ),
            schema_function(
                'get_user',
                'text',
                ['integer'],
                'plpgsql',
                'BEGIN RETURN (SELECT name FROM users WHERE id = $1); END',
            ),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE OR REPLACE FUNCTION get_user(IN int) RETURNS text LANGUAGE plpgsql AS $$BEGIN RETURN (SELECT name FROM users WHERE id = $1); END$$',
            $sqls[0]->toSql(),
        );
    }

    public function test_returns_empty_when_definition_null(): void
    {
        $diff = new FuncDiff(
            schema_function('get_user', 'text', ['integer'], 'sql', 'SELECT name FROM users WHERE id = $1'),
            schema_function('get_user', 'text', ['integer'], 'plpgsql'),
        );

        static::assertSame([], $diff->generate());
    }

    public function test_reversed_function_definition_change(): void
    {
        $diff = new FuncDiff(
            schema_function(
                'get_user',
                'text',
                ['integer'],
                'plpgsql',
                'BEGIN RETURN (SELECT name FROM users WHERE id = $1); END',
            ),
            schema_function('get_user', 'text', ['integer'], 'sql', 'SELECT name FROM users WHERE id = $1'),
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE OR REPLACE FUNCTION get_user(IN int) RETURNS text LANGUAGE sql AS $$SELECT name FROM users WHERE id = $1$$',
            $sqls[0]->toSql(),
        );
    }
}
