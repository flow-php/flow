<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\Exception\SchemaException;
use Flow\PostgreSql\Schema\View;
use Flow\PostgreSql\Schema\ViewDependencyOrder;
use PHPUnit\Framework\TestCase;

final class ViewDependencyOrderTest extends TestCase
{
    public function test_circular_dependency_throws_exception(): void
    {
        $viewA = new View('view_a', 'SELECT * FROM view_b');
        $viewB = new View('view_b', 'SELECT * FROM view_a');

        $this->expectException(SchemaException::class);
        $this->expectExceptionMessage('Circular view dependency');

        (new ViewDependencyOrder(new \Flow\PostgreSql\Parser()))->order([$viewA, $viewB]);
    }

    public function test_empty_list(): void
    {
        static::assertSame([], (new ViewDependencyOrder(new \Flow\PostgreSql\Parser()))->order([]));
    }

    public function test_linear_chain(): void
    {
        $base = new View('base_view', 'SELECT id, name FROM users');
        $middle = new View('middle_view', 'SELECT id FROM base_view WHERE id > 0');
        $top = new View('top_view', 'SELECT * FROM middle_view');

        $result = (new ViewDependencyOrder(new \Flow\PostgreSql\Parser()))->order([$top, $middle, $base]);

        $names = \array_map(static fn(View $v) => $v->name, $result);

        static::assertSame(['base_view', 'middle_view', 'top_view'], $names);
    }

    public function test_self_referencing_view(): void
    {
        $recursive = new View('recursive_view', 'SELECT * FROM recursive_view');
        $other = new View('other_view', 'SELECT * FROM recursive_view');

        $result = (new ViewDependencyOrder(new \Flow\PostgreSql\Parser()))->order([$other, $recursive]);

        $names = \array_map(static fn(View $v) => $v->name, $result);

        static::assertSame(['recursive_view', 'other_view'], $names);
    }

    public function test_single_view(): void
    {
        $view = new View('my_view', 'SELECT * FROM users');

        $result = (new ViewDependencyOrder(new \Flow\PostgreSql\Parser()))->order([$view]);

        static::assertCount(1, $result);
        static::assertSame('my_view', $result[0]->name);
    }

    public function test_view_referencing_external_table_is_not_affected(): void
    {
        $viewA = new View('view_a', 'SELECT * FROM external_table');
        $viewB = new View('view_b', 'SELECT * FROM another_external');

        $result = (new ViewDependencyOrder(new \Flow\PostgreSql\Parser()))->order([$viewA, $viewB]);

        static::assertCount(2, $result);
        static::assertSame('view_a', $result[0]->name);
        static::assertSame('view_b', $result[1]->name);
    }

    public function test_views_without_dependencies_preserve_order(): void
    {
        $viewA = new View('alpha', 'SELECT 1 AS val');
        $viewB = new View('beta', 'SELECT 2 AS val');
        $viewC = new View('gamma', 'SELECT 3 AS val');

        $result = (new ViewDependencyOrder(new \Flow\PostgreSql\Parser()))->order([$viewA, $viewB, $viewC]);

        $names = \array_map(static fn(View $v) => $v->name, $result);

        static::assertSame(['alpha', 'beta', 'gamma'], $names);
    }
}
