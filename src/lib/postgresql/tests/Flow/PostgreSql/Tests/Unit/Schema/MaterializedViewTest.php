<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\IndexMethod;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\agg_count;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class MaterializedViewTest extends TestCase
{
    public function test_materialized_view_construction(): void
    {
        $definition = select(agg_count())->from(table('users'))->toSql();
        $matview = schema_materialized_view('user_stats', $definition);

        static::assertSame('user_stats', $matview->name);
        static::assertSame($definition, $matview->definition);
        static::assertSame([], $matview->indexes);
    }

    public function test_materialized_view_with_indexes(): void
    {
        $matview = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_id', ['id'], unique: true)],
        );

        static::assertCount(1, $matview->indexes);
        static::assertSame('idx_user_stats_id', $matview->indexes[0]->name);
    }

    public function test_to_sql_generates_create_materialized_view(): void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
        )->toSql();

        static::assertCount(1, $sqls);
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id',
            $sqls[0]->toSql(),
        );
    }

    public function test_to_sql_generates_create_materialized_view_with_indexes(): void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_id', ['id'], unique: true)],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id',
            $sqls[0]->toSql(),
        );
        static::assertSame('CREATE UNIQUE INDEX idx_user_stats_id ON user_stats (id)', $sqls[1]->toSql());
    }

    public function test_to_sql_generates_materialized_view_with_multiple_indexes(): void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [
                schema_index('idx_upc_id', ['id'], unique: true),
                schema_index('idx_upc_gin', ['metadata'], method: IndexMethod::GIN),
            ],
        )->toSql();

        static::assertCount(3, $sqls);
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id',
            $sqls[0]->toSql(),
        );
        static::assertSame('CREATE UNIQUE INDEX idx_upc_id ON user_stats (id)', $sqls[1]->toSql());
        static::assertSame('CREATE INDEX idx_upc_gin ON user_stats USING gin (metadata)', $sqls[2]->toSql());
    }

    public function test_to_sql_generates_materialized_view_with_non_btree_index(): void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_gin', ['metadata'], method: IndexMethod::GIN)],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id',
            $sqls[0]->toSql(),
        );
        static::assertSame('CREATE INDEX idx_user_stats_gin ON user_stats USING gin (metadata)', $sqls[1]->toSql());
    }

    public function test_to_sql_generates_materialized_view_with_unique_non_btree_index(): void
    {
        $sqls = schema_materialized_view(
            'user_stats',
            select(col('id'), agg_count())->from(table('users'))->groupBy(col('id'))->toSql(),
            indexes: [schema_index('idx_user_stats_hash', ['id'], unique: true, method: IndexMethod::HASH)],
        )->toSql();

        static::assertCount(2, $sqls);
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT id, count(*) FROM users GROUP BY id',
            $sqls[0]->toSql(),
        );
        static::assertSame('CREATE UNIQUE INDEX idx_user_stats_hash ON user_stats USING hash (id)', $sqls[1]->toSql());
    }
}
