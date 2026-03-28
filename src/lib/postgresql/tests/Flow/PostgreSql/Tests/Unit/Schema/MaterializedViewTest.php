<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use function Flow\PostgreSql\DSL\{agg_count, col, schema_index, schema_materialized_view, select, table};

use Flow\PostgreSql\Schema\IndexMethod;
use PHPUnit\Framework\TestCase;

final class MaterializedViewTest extends TestCase
{
    public function test_materialized_view_construction() : void
    {
        $definition = select(agg_count())->from(table('users'))->toSql();
        $matview = schema_materialized_view('user_stats', $definition);

        self::assertSame('user_stats', $matview->name);
        self::assertSame($definition, $matview->definition);
        self::assertSame([], $matview->indexes);
    }

    public function test_materialized_view_with_indexes() : void
    {
        $matview = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_id', ['id'], unique: true)],
        );

        self::assertCount(1, $matview->indexes);
        self::assertSame('idx_user_stats_id', $matview->indexes[0]->name);
    }

    public function test_to_sql_generates_create_materialized_view() : void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
        )->toSql();

        self::assertCount(1, $sqls);
        self::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_generates_create_materialized_view_with_indexes() : void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_id', ['id'], unique: true)],
        )->toSql();

        self::assertCount(2, $sqls);
        self::assertSame('CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id', $sqls[0]->toSql());
        self::assertSame('CREATE UNIQUE INDEX idx_user_stats_id ON user_stats (id)', $sqls[1]->toSql());
    }

    public function test_to_sql_generates_materialized_view_with_multiple_indexes() : void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [
                schema_index('idx_upc_id', ['id'], unique: true),
                schema_index('idx_upc_gin', ['metadata'], method: IndexMethod::GIN),
            ],
        )->toSql();

        self::assertCount(3, $sqls);
        self::assertSame('CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id', $sqls[0]->toSql());
        self::assertSame('CREATE UNIQUE INDEX idx_upc_id ON user_stats (id)', $sqls[1]->toSql());
        self::assertSame('CREATE INDEX idx_upc_gin ON user_stats USING gin (metadata)', $sqls[2]->toSql());
    }

    public function test_to_sql_generates_materialized_view_with_non_btree_index() : void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_gin', ['metadata'], method: IndexMethod::GIN)],
        )->toSql();

        self::assertCount(2, $sqls);
        self::assertSame('CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id', $sqls[0]->toSql());
        self::assertSame('CREATE INDEX idx_user_stats_gin ON user_stats USING gin (metadata)', $sqls[1]->toSql());
    }

    public function test_to_sql_generates_materialized_view_with_unique_non_btree_index() : void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_hash', ['id'], unique: true, method: IndexMethod::HASH)],
        )->toSql();

        self::assertCount(2, $sqls);
        self::assertSame('CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id', $sqls[0]->toSql());
        self::assertSame('CREATE UNIQUE INDEX idx_user_stats_hash ON user_stats USING hash (id)', $sqls[1]->toSql());
    }
}
