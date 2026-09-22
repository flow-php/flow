<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use Flow\PostgreSql\AST\Visitors\RelationCollector;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Tests\Mother\StopTraversalVisitor;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_parse;

final class RelationCollectorTest extends TestCase
{
    /**
     * @return Generator<string, array{string, list<string>}>
     */
    public static function provide_relation_names(): Generator
    {
        yield 'for update of alias' => ['SELECT * FROM enriched.production p FOR UPDATE OF p', ['enriched.production']];
        yield 'for update of relation name' => ['SELECT * FROM t JOIN u ON true FOR UPDATE OF t', ['t', 'u']];
        yield 'for share of subquery alias' => ['SELECT * FROM (SELECT * FROM t) sub FOR SHARE OF sub', ['t']];
        yield 'cte reference' => ['WITH c AS (SELECT * FROM users) SELECT * FROM c', ['users']];
        yield 'non-recursive self reference' => ['WITH t AS (SELECT * FROM t) SELECT * FROM t', ['t']];
        yield 'earlier sibling' => ['WITH a AS (SELECT * FROM src), b AS (SELECT * FROM a) SELECT * FROM b', ['src']];
        yield 'later sibling is a relation' => ['WITH b AS (SELECT * FROM a), a AS (SELECT 1) SELECT * FROM b', ['a']];
        yield 'recursive later sibling' => [
            'WITH RECURSIVE b AS (SELECT * FROM a), a AS (SELECT 1) SELECT * FROM b',
            [],
        ];
        yield 'recursive self reference' => [
            'WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r',
            [],
        ];
        yield 'with on left union branch' => [
            '(WITH c AS (SELECT 1) SELECT * FROM c) UNION ALL SELECT * FROM c',
            ['c'],
        ];
        yield 'with over union' => ['WITH c AS (SELECT 1) SELECT * FROM c UNION ALL SELECT * FROM c', []];
        yield 'outer cte in sublink' => ['WITH c AS (SELECT 1) SELECT (SELECT * FROM c)', []];
        yield 'subquery cte stays inside' => ['SELECT * FROM (WITH c AS (SELECT 1) SELECT * FROM c) x, c', ['c']];
        yield 'schema-qualified same name' => ['WITH c AS (SELECT 1) SELECT * FROM public.c', ['public.c']];
        yield 'insert target named as cte' => ['WITH c AS (SELECT 1 AS id) INSERT INTO c SELECT id FROM c', ['c']];
        yield 'update from cte' => ['WITH s AS (SELECT 1 AS id) UPDATE t SET id = s.id FROM s', ['t']];
        yield 'update target named as cte' => ['WITH c AS (SELECT 1 AS id) UPDATE c SET id = 2', ['c']];
        yield 'delete using cte' => ['WITH s AS (SELECT 1 AS id) DELETE FROM t USING s WHERE t.id = s.id', ['t']];
        yield 'delete target named as cte' => ['WITH c AS (SELECT 1 AS id) DELETE FROM c', ['c']];
        yield 'merge source cte' => [
            'WITH s AS (SELECT 1 AS id) MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE',
            ['t'],
        ];
        yield 'merge target named as cte' => [
            'WITH t AS (SELECT 1 AS id) MERGE INTO t USING src ON t.id = src.id WHEN MATCHED THEN DELETE',
            ['t', 'src'],
        ];
        yield 'select into named as cte' => ['WITH c AS (SELECT 1 AS id) SELECT id INTO c FROM c', ['c']];
        yield 'ctas' => ['CREATE TABLE x AS SELECT * FROM t', ['t', 'x']];
        yield 'create view' => ['CREATE VIEW v AS SELECT a FROM src', ['v', 'src']];
        yield 'recursive view' => [
            'CREATE RECURSIVE VIEW v (n) AS VALUES (1) UNION ALL SELECT n + 1 FROM v WHERE n < 5',
            ['v'],
        ];
        yield 'data-modifying cte' => [
            'WITH a AS (SELECT 1 AS id), b AS (INSERT INTO a SELECT id FROM a RETURNING id) SELECT * FROM b',
            ['a'],
        ];
        yield 'second statement' => ['WITH c AS (SELECT 1) SELECT * FROM c; SELECT * FROM c', ['c']];
        yield 'quoted name is distinct' => ['WITH "C" AS (SELECT 1) SELECT * FROM c', ['c']];
        yield 'drop tables' => ['DROP TABLE a, s.b', ['a', 's.b']];
        yield 'drop view' => ['DROP VIEW IF EXISTS s.v CASCADE', ['s.v']];
        yield 'drop materialized view' => ['DROP MATERIALIZED VIEW mv', ['mv']];
        yield 'drop foreign table' => ['DROP FOREIGN TABLE ft', ['ft']];
        yield 'drop sequence' => ['DROP SEQUENCE seq', ['seq']];
        yield 'drop index' => ['DROP INDEX CONCURRENTLY s.idx', ['s.idx']];
        yield 'drop trigger' => ['DROP TRIGGER trg ON s.t', ['s.t']];
        yield 'drop rule' => ['DROP RULE r ON t', ['t']];
        yield 'drop policy' => ['DROP POLICY p ON t', ['t']];
        yield 'drop type' => ['DROP TYPE ty', []];
        yield 'drop schema' => ['DROP SCHEMA s', []];
        yield 'comment on table' => ["COMMENT ON TABLE s.t IS 'x'", ['s.t']];
        yield 'comment on column' => ["COMMENT ON COLUMN s.t.c IS 'x'", ['s.t']];
        yield 'comment on constraint' => ["COMMENT ON CONSTRAINT con ON s.t IS 'x'", ['s.t']];
        yield 'comment on domain constraint' => ["COMMENT ON CONSTRAINT dc ON DOMAIN d IS 'x'", []];
        yield 'comment on function' => ["COMMENT ON FUNCTION f() IS 'x'", []];
        yield 'unqualified column comment' => ["COMMENT ON COLUMN c IS 'x'", []];
        yield 'security label on column' => ["SECURITY LABEL FOR p ON COLUMN t.c IS 'x'", ['t']];
        yield 'alter extension add table' => ['ALTER EXTENSION e ADD TABLE s.t', ['s.t']];
        yield 'catalog-qualified drop' => ['DROP TABLE db.s.t', ['s.t']];
        yield 'drop then select' => ['DROP TABLE b; SELECT * FROM a', ['b', 'a']];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    /**
     * @param list<string> $expected
     */
    #[DataProvider('provide_relation_names')]
    public function test_collects_relation_names(string $sql, array $expected): void
    {
        $collector = new RelationCollector();
        sql_parse($sql)->traverse($collector);

        static::assertSame($expected, array_map(
            static fn(RangeVar $rangeVar): string => (
                ($rangeVar->getSchemaname() === '' ? '' : $rangeVar->getSchemaname() . '.') . $rangeVar->getRelname()
            ),
            $collector->getRangeVars(),
        ));
    }

    public function test_name_list_keeps_catalog_name(): void
    {
        $collector = new RelationCollector();
        sql_parse('DROP TABLE db.s.t')->traverse($collector);

        static::assertSame('db', $collector->getRangeVars()[0]->getCatalogname());
    }

    public function test_reset_forgets_cte_scopes_left_open_by_a_stopped_traversal(): void
    {
        $collector = new RelationCollector();
        sql_parse('WITH c AS (SELECT 1) SELECT * FROM c')->traverse($collector, new StopTraversalVisitor());
        $collector->reset();
        sql_parse('SELECT * FROM c')->traverse($collector);

        static::assertSame(
            ['c'],
            array_map(static fn(RangeVar $rangeVar): string => $rangeVar->getRelname(), $collector->getRangeVars()),
        );
    }

    public function test_reset_forgets_collected_relations(): void
    {
        $collector = new RelationCollector();
        sql_parse('SELECT * FROM t')->traverse($collector);
        $collector->reset();
        sql_parse('SELECT * FROM u')->traverse($collector);

        static::assertSame(
            ['u'],
            array_map(static fn(RangeVar $rangeVar): string => $rangeVar->getRelname(), $collector->getRangeVars()),
        );
    }
}
