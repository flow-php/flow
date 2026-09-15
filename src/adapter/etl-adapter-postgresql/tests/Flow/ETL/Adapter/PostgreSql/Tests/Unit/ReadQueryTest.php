<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\ReadQuery;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;
use Flow\PostgreSql\Exception\ParserException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function extension_loaded;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class ReadQueryTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_queries_that_are_not_one_select(): Generator
    {
        yield 'insert returning' => ['INSERT INTO t VALUES (1) RETURNING id'];
        yield 'two statements' => ['SELECT 1; SELECT 2'];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_queries_that_write(): Generator
    {
        yield 'insert cte' => ['WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT id FROM x'];
        yield 'update cte' => ['WITH x AS (UPDATE t SET id = 1 RETURNING id) SELECT id FROM x'];
        yield 'delete cte' => ['WITH x AS (DELETE FROM t RETURNING id) SELECT id FROM x'];
        yield 'merge cte' => [
            'WITH x AS (MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE RETURNING t.id) SELECT id FROM x',
        ];
        yield 'select into' => ['SELECT id INTO TEMPORARY n FROM t'];
        yield 'select into on the first arm of a union' => ['SELECT id INTO TEMPORARY n FROM t UNION SELECT id FROM t'];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            static::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true`',
            );
        }
    }

    public function test_a_query_builder_is_read_as_its_sql(): void
    {
        static::assertSame('SELECT id FROM t', ReadQuery::of(select(col('id'))->from(table('t')), self::class)->sql());
    }

    #[DataProvider('provide_queries_that_write')]
    public function test_a_query_that_writes_is_refused(string $sql): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            self::class
            . ' reads exactly one read-only SELECT or VALUES statement: Expected a read-only SELECT - the query holds a data-modifying WITH or SELECT ... INTO',
        );

        ReadQuery::of($sql, self::class);
    }

    public function test_a_refusal_keeps_the_lib_reason_as_its_cause(): void
    {
        try {
            ReadQuery::of('INSERT INTO t VALUES (1) RETURNING id', self::class);
            static::fail('an INSERT must be refused');
        } catch (InvalidArgumentException $e) {
            static::assertInstanceOf(InvalidStatementException::class, $e->getPrevious());
        }
    }

    #[TestWith(['SELECT id FROM t'])]
    #[TestWith(['SELECT id FROM t;'])]
    #[TestWith(['VALUES (1)'])]
    #[TestWith(['WITH x AS (SELECT id FROM t) SELECT id FROM x'])]
    #[TestWith(['WITH RECURSIVE s(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM s WHERE id < 3) SELECT id FROM s'])]
    public function test_a_single_select_is_read_as_given(string $sql): void
    {
        static::assertSame($sql, ReadQuery::of($sql, self::class)->sql());
    }

    public function test_a_syntax_error_is_a_parser_exception(): void
    {
        $this->expectException(ParserException::class);

        ReadQuery::of('SELEC id FROM t', self::class);
    }

    #[DataProvider('provide_queries_that_are_not_one_select')]
    public function test_anything_but_one_select_is_refused(string $sql): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(self::class
        . ' reads exactly one read-only SELECT or VALUES statement: Expected exactly one SELECT or VALUES statement');

        ReadQuery::of($sql, self::class);
    }

    public function test_count_wraps_the_query_without_its_order(): void
    {
        static::assertSame(
            'SELECT count(*) FROM (SELECT id FROM t WHERE id > $1) _count_subq',
            ReadQuery::of('SELECT id FROM t WHERE id > $1 ORDER BY id', self::class)->count(),
        );
    }

    public function test_declare_cursor_wraps_the_query(): void
    {
        static::assertSame(
            'DECLARE c NO SCROLL CURSOR FOR SELECT id FROM t WHERE id > $1 ORDER BY id',
            ReadQuery::of('SELECT id FROM t WHERE id > $1 ORDER BY id', self::class)->declareCursor('c')->toSql(),
        );
    }

    public function test_every_query_is_built_from_the_query_as_given(): void
    {
        $read = ReadQuery::of('SELECT id FROM t ORDER BY id', self::class);

        $page = $read->page(1);

        static::assertSame($page, $read->page(1));
        static::assertSame('SELECT count(*) FROM (SELECT id FROM t) _count_subq', $read->count());
        static::assertSame(
            'DECLARE c NO SCROLL CURSOR FOR SELECT id FROM t ORDER BY id',
            $read->declareCursor('c')->toSql(),
        );
    }

    #[TestWith(['SELECT id FROM t ORDER BY id', true])]
    #[TestWith(['SELECT id FROM t UNION SELECT id FROM u ORDER BY id', true])]
    #[TestWith(['SELECT id FROM t', false])]
    #[TestWith(['SELECT * FROM (SELECT id FROM t ORDER BY id) s', false])]
    public function test_is_ordered_reads_only_the_top_level_order(string $sql, bool $ordered): void
    {
        static::assertSame($ordered, ReadQuery::of($sql, self::class)->isOrdered());
    }

    public function test_key_set_first_page_limits_through_the_placeholder_after_the_callers(): void
    {
        static::assertSame('SELECT id FROM t WHERE active = $1 ORDER BY id ASC LIMIT $2', ReadQuery::of(
            'SELECT id FROM t WHERE active = $1',
            self::class,
        )->keySetFirstPage(pgsql_pagination_key_set(pgsql_pagination_key_asc('id')), 2));
    }

    public function test_key_set_next_page_takes_the_last_key_after_the_limit(): void
    {
        static::assertSame('SELECT id FROM t WHERE active = $1 AND id > $3 ORDER BY id ASC LIMIT $2', ReadQuery::of(
            'SELECT id FROM t WHERE active = $1',
            self::class,
        )->keySetNextPage(pgsql_pagination_key_set(pgsql_pagination_key_asc('id')), 2));
    }

    public function test_page_limits_and_offsets_through_the_placeholders_after_the_callers(): void
    {
        static::assertSame(
            'SELECT id FROM t WHERE id > $1 ORDER BY id LIMIT $2 OFFSET $3',
            ReadQuery::of('SELECT id FROM t WHERE id > $1 ORDER BY id', self::class)->page(2),
        );
    }
}
