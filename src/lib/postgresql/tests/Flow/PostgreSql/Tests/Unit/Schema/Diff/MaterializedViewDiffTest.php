<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use function Flow\PostgreSql\DSL\{col, eq, literal, schema_index, schema_materialized_view, select, star, table};

use Flow\PostgreSql\Schema\Diff\MaterializedViewDiff;

use Flow\PostgreSql\Schema\IndexMethod;
use PHPUnit\Framework\TestCase;

final class MaterializedViewDiffTest extends TestCase
{
    public function test_adds_and_drops_indexes_without_definition_change() : void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            addedIndexes: [schema_index('idx_email', ['email'])],
            removedIndexes: [schema_index('idx_name', ['name'])],
        );

        $sqls = $diff->generate();

        self::assertCount(2, $sqls);
        self::assertSame('DROP INDEX idx_name', $sqls[0]->toSql());
        self::assertSame('CREATE INDEX idx_email ON mv_users (email)', $sqls[1]->toSql());
    }

    public function test_adds_gin_index_without_definition_change() : void
    {
        $source = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());
        $target = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());

        $diff = new MaterializedViewDiff(
            $source,
            $target,
            addedIndexes: [schema_index('idx_data', ['data'], method: IndexMethod::GIN)],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE INDEX idx_data ON mv_users USING gin (data)', $sqls[0]->toSql());
    }

    public function test_adds_index() : void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            addedIndexes: [schema_index('idx_email', ['email'])],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE INDEX idx_email ON mv_users (email)', $sqls[0]->toSql());
    }

    public function test_adds_unique_index_without_definition_change() : void
    {
        $source = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());
        $target = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());

        $diff = new MaterializedViewDiff(
            $source,
            $target,
            addedIndexes: [schema_index('idx_unique_email', ['email'], unique: true)],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('CREATE UNIQUE INDEX idx_unique_email ON mv_users (email)', $sqls[0]->toSql());
    }

    public function test_drops_index() : void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            removedIndexes: [schema_index('idx_email', ['email'])],
        );

        $sqls = $diff->generate();

        self::assertCount(1, $sqls);
        self::assertSame('DROP INDEX idx_email', $sqls[0]->toSql());
    }

    public function test_drops_source_indexes_before_recreating_when_definition_changed() : void
    {
        $sourceDefinition = select(star())->from(table('users'))->toSql();
        $targetDefinition = select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $sourceDefinition, [schema_index('idx_name', ['name'])]),
            schema_materialized_view('mv_users', $targetDefinition, [schema_index('idx_email', ['email'])]),
        );

        $sqls = $diff->generate();

        self::assertCount(4, $sqls);
        self::assertSame('DROP INDEX idx_name', $sqls[0]->toSql());
        self::assertSame('DROP MATERIALIZED VIEW mv_users', $sqls[1]->toSql());
        self::assertSame('CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users WHERE active = true', $sqls[2]->toSql());
        self::assertSame('CREATE INDEX idx_email ON mv_users (email)', $sqls[3]->toSql());
    }

    public function test_has_definition_changed_returns_false_when_same() : void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
        );

        self::assertFalse($diff->hasDefinitionChanged());
    }

    public function test_has_definition_changed_returns_true_when_different() : void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            schema_materialized_view('mv_users', select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql()),
        );

        self::assertTrue($diff->hasDefinitionChanged());
    }

    public function test_recreates_when_definition_changed() : void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            schema_materialized_view('mv_users', select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql()),
        );

        $sqls = $diff->generate();

        self::assertCount(2, $sqls);
        self::assertSame('DROP MATERIALIZED VIEW mv_users', $sqls[0]->toSql());
        self::assertSame('CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users WHERE active = true', $sqls[1]->toSql());
    }

    public function test_reversed_definition_change() : void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql()),
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
        );

        $sqls = $diff->generate();

        self::assertCount(2, $sqls);
        self::assertSame('DROP MATERIALIZED VIEW mv_users', $sqls[0]->toSql());
        self::assertSame('CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users', $sqls[1]->toSql());
    }

    public function test_reversed_index_only_changes() : void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            addedIndexes: [schema_index('idx_name', ['name'])],
            removedIndexes: [schema_index('idx_email', ['email'])],
        );

        $sqls = $diff->generate();

        self::assertCount(2, $sqls);
        self::assertSame('DROP INDEX idx_email', $sqls[0]->toSql());
        self::assertSame('CREATE INDEX idx_name ON mv_users (name)', $sqls[1]->toSql());
    }
}
