<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Cursor;

use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\declare_cursor;
use function Flow\PostgreSql\DSL\sql_parse;

final class DeclareCursorBuilderTest extends TestCase
{
    public function test_a_non_select_statement_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DECLARE CURSOR takes exactly one SELECT or VALUES statement');

        declare_cursor('c', 'INSERT INTO t VALUES (1) RETURNING id');
    }

    public function test_a_parsed_non_select_statement_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DECLARE CURSOR takes exactly one SELECT or VALUES statement');

        declare_cursor('c', sql_parse('INSERT INTO t VALUES (1) RETURNING id'));
    }

    public function test_a_parsed_query_declares(): void
    {
        static::assertSame(
            'DECLARE c NO SCROLL CURSOR FOR SELECT id FROM t WHERE id > $1',
            declare_cursor('c', sql_parse('SELECT id FROM t WHERE id > $1'))->toSql(),
        );
    }

    public function test_a_table_statement_declares_as_a_select(): void
    {
        static::assertSame('DECLARE c NO SCROLL CURSOR FOR SELECT * FROM t', declare_cursor('c', 'TABLE t')->toSql());
    }

    public function test_a_values_statement_declares(): void
    {
        static::assertSame('DECLARE c NO SCROLL CURSOR FOR VALUES (1)', declare_cursor('c', 'VALUES (1)')->toSql());
    }

    public function test_more_than_one_statement_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('DECLARE CURSOR takes exactly one SELECT or VALUES statement');

        declare_cursor('c', 'SELECT 1; SELECT 2');
    }
}
