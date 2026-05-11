<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_primary_key;

final class PrimaryKeyTest extends TestCase
{
    public function test_is_equal_for_identical_primary_keys(): void
    {
        $a = schema_primary_key(['id'], 'pk_users');
        $b = schema_primary_key(['id'], 'pk_users');

        static::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_columns_differ(): void
    {
        $a = schema_primary_key(['id'], 'pk_users');
        $b = schema_primary_key(['id', 'tenant_id'], 'pk_users');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs(): void
    {
        $a = schema_primary_key(['id'], 'pk_a');
        $b = schema_primary_key(['id'], 'pk_b');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_columns_differ(): void
    {
        $a = schema_primary_key(['id']);
        $b = schema_primary_key(['user_id']);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_for_columns_in_different_order(): void
    {
        $a = schema_primary_key(['id', 'tenant_id'], 'pk_users');
        $b = schema_primary_key(['tenant_id', 'id'], 'pk_users');

        static::assertTrue($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs(): void
    {
        $a = schema_primary_key(['id', 'tenant_id'], 'pk_a');
        $b = schema_primary_key(['id', 'tenant_id'], 'pk_b');

        static::assertTrue($a->isEqualStructure($b));
    }

    public function test_primary_key_construction(): void
    {
        $pk = schema_primary_key(['id']);

        static::assertSame(['id'], $pk->columns);
        static::assertNull($pk->name);
    }

    public function test_primary_key_with_name(): void
    {
        $pk = schema_primary_key(['id', 'tenant_id'], 'pk_users');

        static::assertSame(['id', 'tenant_id'], $pk->columns);
        static::assertSame('pk_users', $pk->name);
    }
}
