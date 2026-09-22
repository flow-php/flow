<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use Flow\PostgreSql\AST\Visitors\ParamRefCollector;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_parse;
use function max;
use function pg_query_parse;
use function preg_match_all;
use function substr_count;

final class ParamRefCollectorTest extends TestCase
{
    /**
     * @return \Generator<string, array{string}>
     */
    public static function parserQueries(): Generator
    {
        yield 'ON CONFLICT DO UPDATE' => [
            'INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET b = $1 WHERE t.c = $2',
        ];
        yield 'MERGE' => ['MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET a = $1'];
        yield 'EXPLAIN' => ['EXPLAIN SELECT * FROM t WHERE a = $1'];
        yield 'array subscript' => ['SELECT arr[$1] FROM t'];
        yield 'array slice' => ['SELECT arr[$1:$2] FROM t'];
        yield 'UPDATE SET (a,b)=(..)' => ['UPDATE t SET (a, b) = ($1, $2)'];
        yield 'UPDATE SET (a,b)=sub' => ['UPDATE t SET (a, b) = (SELECT x, y FROM u WHERE z = $1)'];
        yield 'WINDOW clause' => ['SELECT count(*) OVER w FROM t WINDOW w AS (PARTITION BY $1::text)'];
        yield 'CALL' => ['CALL p($1)'];
        yield 'CREATE TABLE AS' => ['CREATE TABLE x AS SELECT * FROM t WHERE a = $1'];
        yield 'COPY query' => ['COPY (SELECT * FROM t WHERE a = $1) TO STDOUT'];
        yield 'DECLARE CURSOR' => ['DECLARE c CURSOR FOR SELECT * FROM t WHERE a = $1'];
        yield 'TABLESAMPLE' => ['SELECT * FROM t TABLESAMPLE BERNOULLI ($1)'];
        yield 'LATERAL subquery' => ['SELECT * FROM t, LATERAL (SELECT * FROM u WHERE u.a = $1) s'];
        yield 'DISTINCT ON' => ['SELECT DISTINCT ON ($1::int) a FROM t'];
        yield 'FETCH FIRST' => ['SELECT * FROM t FETCH FIRST $1 ROWS ONLY'];
        yield 'UNION + LIMIT' => ['SELECT a FROM t UNION SELECT a FROM u ORDER BY 1 LIMIT $1'];
        yield 'GROUPING() func' => ['SELECT GROUPING(a) FROM t GROUP BY ROLLUP (a, $1::text)'];
        yield 'ROLLUP' => ['SELECT a FROM t GROUP BY ROLLUP (a, $1::text)'];
        yield 'CUBE' => ['SELECT a FROM t GROUP BY CUBE (a, $1::text)'];
        yield 'field select ($1).f' => ['SELECT ($1::t).f'];
        yield 'LIKE ESCAPE' => ['SELECT * FROM t WHERE a LIKE $1 ESCAPE $2'];
        yield 'BETWEEN' => ['SELECT * FROM t WHERE a BETWEEN $1 AND $2'];
        yield 'IN list' => ['SELECT * FROM t WHERE a IN ($1, $2)'];
        yield 'EXISTS sublink' => ['SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.a = $1)'];
        yield 'row compare sublink' => ['SELECT * FROM t WHERE (a, b) IN (SELECT $1::int, $2::int)'];
        yield 'ARRAY sublink' => ['SELECT ARRAY(SELECT a FROM u WHERE b = $1)'];
        yield 'IS JSON' => ['SELECT $1::text IS JSON'];
        yield 'JSON_OBJECT' => ["SELECT JSON_OBJECT('a' : \$1::int)"];
        yield 'JSON_VALUE' => ["SELECT JSON_VALUE(\$1::jsonb, '\$.a')"];
        yield 'JSON_TABLE' => ["SELECT * FROM JSON_TABLE(\$1::jsonb, '\$[*]' COLUMNS (a int PATH '\$.a')) jt"];
        yield 'XMLSERIALIZE' => ['SELECT XMLSERIALIZE(CONTENT $1::xml AS text)'];
        yield 'ROWS FROM' => ['SELECT * FROM ROWS FROM (generate_series(1, $1))'];
        yield 'func in FROM' => ['SELECT * FROM generate_series(1, $1)'];
        yield 'VALUES in FROM' => ['SELECT * FROM (VALUES ($1::int)) v(a)'];
        yield 'CTE' => ['WITH c AS (SELECT $1::int AS a) SELECT * FROM c'];
        yield 'CTE in UPDATE FROM' => ['UPDATE t SET a = 1 FROM u WHERE u.b = $1'];
        yield 'DELETE USING' => ['DELETE FROM t USING u WHERE u.b = $1'];
        yield 'RETURNING' => ['DELETE FROM t RETURNING a + $1'];
        yield 'INSERT SELECT' => ['INSERT INTO t SELECT * FROM u WHERE a = $1'];
        yield 'agg ORDER BY' => ['SELECT string_agg(a, $1 ORDER BY $2::int) FROM t'];
        yield 'WITHIN GROUP' => ['SELECT percentile_cont($1::float) WITHIN GROUP (ORDER BY a) FROM t'];
        yield 'FILTER' => ['SELECT count(*) FILTER (WHERE a = $1) FROM t'];
        yield 'JOIN ON' => ['SELECT * FROM t JOIN u ON t.a = u.a AND u.b = $1'];
        yield 'CASE' => ['SELECT CASE WHEN a = $1 THEN $2 ELSE $3 END FROM t'];
        yield 'COALESCE/NULLIF' => ['SELECT COALESCE(a, $1), NULLIF(a, $2) FROM t'];
        yield 'GREATEST' => ['SELECT GREATEST(a, $1) FROM t'];
        yield 'IS NULL/IS TRUE' => ['SELECT * FROM t WHERE $1::int IS NULL AND $2::bool IS TRUE'];
        yield 'ROW()' => ['SELECT ROW($1, $2)'];
        yield 'ARRAY[]' => ['SELECT ARRAY[$1, $2]'];
        yield 'named arg' => ['SELECT f(a => $1)'];
        yield 'SQLValueFunction+AT TZ' => ['SELECT now() AT TIME ZONE $1'];
        yield 'PREPARE' => ['PREPARE p AS SELECT * FROM t WHERE a = $1'];
        yield 'VIEW' => ['CREATE VIEW v AS SELECT * FROM t WHERE a = $1'];
        yield 'SELECT INTO' => ['SELECT * INTO x FROM t WHERE a = $1'];
        yield 'FOR UPDATE' => ['SELECT * FROM t WHERE a = $1 FOR UPDATE'];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    #[DataProvider('parserQueries')]
    public function test_collects_every_param_ref_the_parser_produces(string $sql): void
    {
        $collector = new ParamRefCollector();
        sql_parse($sql)->traverse($collector);

        $numbers = [];
        preg_match_all('/\\$(\\d+)/', $sql, $numbers);

        static::assertCount(substr_count(pg_query_parse($sql), '"ParamRef"'), $collector->getParamRefs());
        static::assertSame(max(array_map(intval(...), $numbers[1])), $collector->getMaxParamNumber());
    }

    public function test_collects_multiple_params(): void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE status = $1 AND category = $2 AND created_at > $3');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        static::assertCount(3, $collector->getParamRefs());
        static::assertSame(3, $collector->getMaxParamNumber());
    }

    public function test_collects_params_inside_between(): void
    {
        $parsed = sql_parse('SELECT * FROM t WHERE t.d BETWEEN $1 AND $2');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        static::assertCount(2, $collector->getParamRefs());
        static::assertSame(2, $collector->getMaxParamNumber());
    }

    public function test_collects_params_inside_in_list(): void
    {
        $parsed = sql_parse('SELECT * FROM t WHERE t.s IN ($1, $2, $3)');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        static::assertCount(3, $collector->getParamRefs());
        static::assertSame(3, $collector->getMaxParamNumber());
    }

    public function test_collects_no_params_from_query_without_placeholders(): void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE active = true');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        static::assertSame([], $collector->getParamRefs());
        static::assertSame(0, $collector->getMaxParamNumber());
    }

    public function test_collects_single_param(): void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id = $1');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        static::assertCount(1, $collector->getParamRefs());
        static::assertSame(1, $collector->getMaxParamNumber());
    }

    public function test_finds_max_param_number_with_gaps(): void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id = $3 AND status = $1');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        static::assertCount(2, $collector->getParamRefs());
        static::assertSame(3, $collector->getMaxParamNumber());
    }

    public function test_reset_clears_collected_params(): void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id = $1');

        $collector = new ParamRefCollector();
        $parsed->traverse($collector);

        static::assertCount(1, $collector->getParamRefs());

        $collector->reset();

        static::assertSame([], $collector->getParamRefs());
        static::assertSame(0, $collector->getMaxParamNumber());
    }
}
