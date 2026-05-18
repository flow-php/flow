<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Schema\Exception\SchemaException;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\MaterializedViewDependencyOrder;
use PHPUnit\Framework\TestCase;

use function array_map;

final class MaterializedViewDependencyOrderTest extends TestCase
{
    public function test_circular_dependency_throws_exception(): void
    {
        $viewA = new MaterializedView('mv_a', 'SELECT * FROM mv_b');
        $viewB = new MaterializedView('mv_b', 'SELECT * FROM mv_a');

        $this->expectException(SchemaException::class);
        $this->expectExceptionMessage('Circular dependency detected between materialized views');

        (new MaterializedViewDependencyOrder(new Parser()))->order([$viewA, $viewB]);
    }

    public function test_empty_list(): void
    {
        static::assertSame([], (new MaterializedViewDependencyOrder(new Parser()))->order([]));
    }

    public function test_linear_chain(): void
    {
        $base = new MaterializedView('base_mv', 'SELECT id, name FROM users');
        $top = new MaterializedView('top_mv', 'SELECT * FROM base_mv');

        $result = (new MaterializedViewDependencyOrder(new Parser()))->order([$top, $base]);

        $names = array_map(static fn(MaterializedView $v) => $v->name, $result);

        static::assertSame(['base_mv', 'top_mv'], $names);
    }

    public function test_self_referencing_materialized_view(): void
    {
        $recursive = new MaterializedView('recursive_mv', 'SELECT * FROM recursive_mv');
        $other = new MaterializedView('other_mv', 'SELECT * FROM recursive_mv');

        $result = (new MaterializedViewDependencyOrder(new Parser()))->order([$other, $recursive]);

        $names = array_map(static fn(MaterializedView $v) => $v->name, $result);

        static::assertSame(['recursive_mv', 'other_mv'], $names);
    }

    public function test_single_materialized_view(): void
    {
        $view = new MaterializedView('my_mv', 'SELECT * FROM users');

        $result = (new MaterializedViewDependencyOrder(new Parser()))->order([$view]);

        static::assertCount(1, $result);
        static::assertSame('my_mv', $result[0]->name);
    }

    public function test_views_without_dependencies_preserve_order(): void
    {
        $viewA = new MaterializedView('alpha', 'SELECT 1 AS val');
        $viewB = new MaterializedView('beta', 'SELECT 2 AS val');

        $result = (new MaterializedViewDependencyOrder(new Parser()))->order([$viewA, $viewB]);

        $names = array_map(static fn(MaterializedView $v) => $v->name, $result);

        static::assertSame(['alpha', 'beta'], $names);
    }
}
