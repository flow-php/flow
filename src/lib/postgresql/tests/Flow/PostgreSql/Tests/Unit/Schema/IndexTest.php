<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema;

use Flow\PostgreSql\Schema\IndexMethod;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_index;

final class IndexTest extends TestCase
{
    public function test_index_construction(): void
    {
        $index = schema_index('idx_users_name', ['name']);

        static::assertSame('idx_users_name', $index->name);
        static::assertSame(['name'], $index->columns);
        static::assertFalse($index->unique);
        static::assertSame(IndexMethod::BTREE, $index->method);
        static::assertFalse($index->primary);
        static::assertNull($index->predicate);
    }

    public function test_index_with_method(): void
    {
        $index = schema_index('idx_data_tags', ['tags'], method: IndexMethod::GIN);

        static::assertSame(IndexMethod::GIN, $index->method);
    }

    public function test_index_with_predicate(): void
    {
        $index = schema_index('idx_active_users', ['email'], predicate: 'active = true');

        static::assertSame('active = true', $index->predicate);
    }

    public function test_is_equal_for_identical_indexes(): void
    {
        $a = schema_index('idx_users_name', ['name'], unique: true, method: IndexMethod::BTREE);
        $b = schema_index('idx_users_name', ['name'], unique: true, method: IndexMethod::BTREE);

        static::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_columns_differ(): void
    {
        $a = schema_index('idx_users', ['name']);
        $b = schema_index('idx_users', ['email']);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_method_differs(): void
    {
        $a = schema_index('idx_users', ['name'], method: IndexMethod::BTREE);
        $b = schema_index('idx_users', ['name'], method: IndexMethod::GIN);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs(): void
    {
        $a = schema_index('idx_a', ['name']);
        $b = schema_index('idx_b', ['name']);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_predicate_differs(): void
    {
        $a = schema_index('idx_users', ['name'], predicate: 'active = true');
        $b = schema_index('idx_users', ['name'], predicate: 'active = false');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_unique_differs(): void
    {
        $a = schema_index('idx_users', ['name'], unique: false);
        $b = schema_index('idx_users', ['name'], unique: true);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_columns_differ(): void
    {
        $a = schema_index('idx_users', ['name']);
        $b = schema_index('idx_users', ['email']);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_method_differs(): void
    {
        $a = schema_index('idx_users', ['name'], method: IndexMethod::BTREE);
        $b = schema_index('idx_users', ['name'], method: IndexMethod::HASH);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_primary_differs(): void
    {
        $a = schema_index('idx_users', ['id'], primary: true);
        $b = schema_index('idx_users', ['id'], primary: false);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs(): void
    {
        $a = schema_index(
            'idx_a',
            ['name', 'email'],
            unique: true,
            method: IndexMethod::GIN,
            predicate: 'active = true',
        );
        $b = schema_index(
            'idx_b',
            ['name', 'email'],
            unique: true,
            method: IndexMethod::GIN,
            predicate: 'active = true',
        );

        static::assertTrue($a->isEqualStructure($b));
    }

    public function test_unique_index(): void
    {
        $index = schema_index('idx_users_email', ['email'], unique: true);

        static::assertTrue($index->unique);
    }
}
