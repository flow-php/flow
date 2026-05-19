<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\Exception\SchemaException;
use Flow\PostgreSql\Schema\ForeignKeyDependencyOrder;
use PHPUnit\Framework\TestCase;

use function array_map;
use function array_search;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_foreign_key;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;

final class ForeignKeyDependencyOrderTest extends TestCase
{
    public function test_circular_dependency_throws_exception(): void
    {
        $tableA = schema_table(
            'table_a',
            [schema_column_integer('id'), schema_column_integer('b_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['b_id'], 'table_b', ['id'])],
            schema: 'public',
        );

        $tableB = schema_table(
            'table_b',
            [schema_column_integer('id'), schema_column_integer('a_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['a_id'], 'table_a', ['id'])],
            schema: 'public',
        );

        $this->expectException(SchemaException::class);
        $this->expectExceptionMessage('Circular foreign key dependency');

        (new ForeignKeyDependencyOrder())->order([$tableA, $tableB]);
    }

    public function test_diamond_dependency(): void
    {
        $root = schema_table('root', [schema_column_integer('id')], schema_primary_key(['id']), schema: 'public');

        $left = schema_table(
            'left_table',
            [schema_column_integer('id'), schema_column_integer('root_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['root_id'], 'root', ['id'])],
            schema: 'public',
        );

        $right = schema_table(
            'right_table',
            [schema_column_integer('id'), schema_column_integer('root_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['root_id'], 'root', ['id'])],
            schema: 'public',
        );

        $bottom = schema_table(
            'bottom',
            [schema_column_integer('id'), schema_column_integer('left_id'), schema_column_integer('right_id')],
            schema_primary_key(['id']),
            foreignKeys: [
                schema_foreign_key(['left_id'], 'left_table', ['id']),
                schema_foreign_key(['right_id'], 'right_table', ['id']),
            ],
            schema: 'public',
        );

        $result = (new ForeignKeyDependencyOrder())->order([$bottom, $left, $right, $root]);

        $names = array_map(static fn($t) => $t->name, $result);

        static::assertSame('root', $names[0]);
        $leftIdx = array_search('left_table', $names, true);
        $rightIdx = array_search('right_table', $names, true);
        $bottomIdx = array_search('bottom', $names, true);
        static::assertGreaterThan($leftIdx, $bottomIdx);
        static::assertGreaterThan($rightIdx, $bottomIdx);
    }

    public function test_empty_list(): void
    {
        static::assertSame([], (new ForeignKeyDependencyOrder())->order([]));
    }

    public function test_fk_referencing_table_outside_the_list_is_ignored(): void
    {
        $independent = schema_table(
            'independent',
            [schema_column_integer('id')],
            schema_primary_key(['id']),
            schema: 'public',
        );

        $child = schema_table(
            'child',
            [schema_column_integer('id'), schema_column_integer('external_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(
                ['external_id'],
                'external_table',
                ['id'],
                referenceSchema: 'other_schema',
            )],
            schema: 'public',
        );

        $result = (new ForeignKeyDependencyOrder())->order([$independent, $child]);

        static::assertCount(2, $result);
    }

    public function test_linear_chain(): void
    {
        $grandparent = schema_table(
            'grandparent',
            [schema_column_integer('id')],
            schema_primary_key(['id']),
            schema: 'public',
        );

        $parent = schema_table(
            'parent',
            [schema_column_integer('id'), schema_column_integer('gp_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['gp_id'], 'grandparent', ['id'])],
            schema: 'public',
        );

        $child = schema_table(
            'child',
            [schema_column_integer('id'), schema_column_integer('parent_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['parent_id'], 'parent', ['id'])],
            schema: 'public',
        );

        $result = (new ForeignKeyDependencyOrder())->order([$child, $parent, $grandparent]);

        $names = array_map(static fn($t) => $t->name, $result);

        static::assertSame(['grandparent', 'parent', 'child'], $names);
    }

    public function test_self_referencing_fk(): void
    {
        $categories = schema_table(
            'categories',
            [schema_column_integer('id'), schema_column_integer('parent_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['parent_id'], 'categories', ['id'])],
            schema: 'public',
        );

        $products = schema_table(
            'products',
            [schema_column_integer('id'), schema_column_integer('category_id')],
            schema_primary_key(['id']),
            foreignKeys: [schema_foreign_key(['category_id'], 'categories', ['id'])],
            schema: 'public',
        );

        $result = (new ForeignKeyDependencyOrder())->order([$products, $categories]);

        $names = array_map(static fn($t) => $t->name, $result);

        static::assertSame(['categories', 'products'], $names);
    }

    public function test_single_table(): void
    {
        $table = schema_table(
            'users',
            [schema_column_integer('id'), schema_column_text('name')],
            schema_primary_key(['id']),
            schema: 'public',
        );

        $result = (new ForeignKeyDependencyOrder())->order([$table]);

        static::assertCount(1, $result);
        static::assertSame('users', $result[0]->name);
    }

    public function test_tables_without_fks_preserve_order(): void
    {
        $tableA = schema_table('alpha', [schema_column_integer('id')], schema_primary_key(['id']), schema: 'public');

        $tableB = schema_table('beta', [schema_column_integer('id')], schema_primary_key(['id']), schema: 'public');

        $tableC = schema_table('gamma', [schema_column_integer('id')], schema_primary_key(['id']), schema: 'public');

        $result = (new ForeignKeyDependencyOrder())->order([$tableA, $tableB, $tableC]);

        $names = array_map(static fn($t) => $t->name, $result);

        static::assertSame(['alpha', 'beta', 'gamma'], $names);
    }
}
