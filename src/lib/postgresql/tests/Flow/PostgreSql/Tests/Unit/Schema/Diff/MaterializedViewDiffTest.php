<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Diff\MaterializedViewDiff;
use Flow\PostgreSql\Schema\IndexMethod;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class MaterializedViewDiffTest extends TestCase
{
    public function test_adds_and_drops_indexes_without_definition_change(): void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            addedIndexes: [schema_index('idx_email', ['email'])],
            removedIndexes: [schema_index('idx_name', ['name'])],
        );

        $sqls = $diff->generate();

        static::assertCount(2, $sqls);
        static::assertSame('DROP INDEX idx_name', $sqls[0]->toSql());
        static::assertSame('CREATE INDEX idx_email ON mv_users (email)', $sqls[1]->toSql());
    }

    public function test_adds_gin_index_without_definition_change(): void
    {
        $source = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());
        $target = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());

        $diff = new MaterializedViewDiff($source, $target, addedIndexes: [schema_index(
            'idx_data',
            ['data'],
            method: IndexMethod::GIN,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE INDEX idx_data ON mv_users USING gin (data)', $sqls[0]->toSql());
    }

    public function test_adds_index(): void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            addedIndexes: [schema_index('idx_email', ['email'])],
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE INDEX idx_email ON mv_users (email)', $sqls[0]->toSql());
    }

    public function test_adds_unique_index_without_definition_change(): void
    {
        $source = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());
        $target = schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql());

        $diff = new MaterializedViewDiff($source, $target, addedIndexes: [schema_index(
            'idx_unique_email',
            ['email'],
            unique: true,
        )]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE UNIQUE INDEX idx_unique_email ON mv_users (email)', $sqls[0]->toSql());
    }

    public function test_drops_index(): void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            removedIndexes: [schema_index('idx_email', ['email'])],
        );

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP INDEX idx_email', $sqls[0]->toSql());
    }

    public function test_drops_source_indexes_before_recreating_when_definition_changed(): void
    {
        $sourceDefinition = select(star())->from(table('users'))->toSql();
        $targetDefinition = select(star())
            ->from(table('users'))
            ->where(eq(col('active'), literal(true)))
            ->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $sourceDefinition, [schema_index('idx_name', ['name'])]),
            schema_materialized_view('mv_users', $targetDefinition, [schema_index('idx_email', ['email'])]),
        );

        $sqls = $diff->generate();

        static::assertCount(4, $sqls);
        static::assertSame('DROP INDEX idx_name', $sqls[0]->toSql());
        static::assertSame('DROP MATERIALIZED VIEW mv_users', $sqls[1]->toSql());
        static::assertSame(
            'CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users WHERE active = true',
            $sqls[2]->toSql(),
        );
        static::assertSame('CREATE INDEX idx_email ON mv_users (email)', $sqls[3]->toSql());
    }

    public function test_has_definition_changed_returns_false_when_same(): void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
        );

        static::assertFalse($diff->hasDefinitionChanged());
    }

    public function test_has_definition_changed_returns_true_when_different(): void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            schema_materialized_view(
                'mv_users',
                select(star())
                    ->from(table('users'))
                    ->where(eq(col('active'), literal(true)))
                    ->toSql(),
            ),
        );

        static::assertTrue($diff->hasDefinitionChanged());
    }

    public function test_recreates_when_definition_changed(): void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
            schema_materialized_view(
                'mv_users',
                select(star())
                    ->from(table('users'))
                    ->where(eq(col('active'), literal(true)))
                    ->toSql(),
            ),
        );

        $sqls = $diff->generate();

        static::assertCount(2, $sqls);
        static::assertSame('DROP MATERIALIZED VIEW mv_users', $sqls[0]->toSql());
        static::assertSame(
            'CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users WHERE active = true',
            $sqls[1]->toSql(),
        );
    }

    public function test_reversed_definition_change(): void
    {
        $diff = new MaterializedViewDiff(
            schema_materialized_view(
                'mv_users',
                select(star())
                    ->from(table('users'))
                    ->where(eq(col('active'), literal(true)))
                    ->toSql(),
            ),
            schema_materialized_view('mv_users', select(star())->from(table('users'))->toSql()),
        );

        $sqls = $diff->generate();

        static::assertCount(2, $sqls);
        static::assertSame('DROP MATERIALIZED VIEW mv_users', $sqls[0]->toSql());
        static::assertSame('CREATE MATERIALIZED VIEW mv_users AS SELECT * FROM users', $sqls[1]->toSql());
    }

    public function test_reversed_index_only_changes(): void
    {
        $definition = select(star())->from(table('users'))->toSql();

        $diff = new MaterializedViewDiff(
            schema_materialized_view('mv_users', $definition),
            schema_materialized_view('mv_users', $definition),
            addedIndexes: [schema_index('idx_name', ['name'])],
            removedIndexes: [schema_index('idx_email', ['email'])],
        );

        $sqls = $diff->generate();

        static::assertCount(2, $sqls);
        static::assertSame('DROP INDEX idx_email', $sqls[0]->toSql());
        static::assertSame('CREATE INDEX idx_name ON mv_users (name)', $sqls[1]->toSql());
    }
}
