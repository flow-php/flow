<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_foreign_key;

final class ForeignKeyTest extends TestCase
{
    public function test_foreign_key_construction(): void
    {
        $fk = schema_foreign_key(['user_id'], 'users', ['id']);

        static::assertSame(['user_id'], $fk->columns);
        static::assertSame('users', $fk->referenceTable);
        static::assertSame(['id'], $fk->referenceColumns);
        static::assertSame('public', $fk->referenceSchema);
        static::assertNull($fk->name);
    }

    public function test_foreign_key_deferrable(): void
    {
        $fk = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true, initiallyDeferred: true);

        static::assertTrue($fk->deferrable);
        static::assertTrue($fk->initiallyDeferred);
    }

    public function test_foreign_key_with_actions(): void
    {
        $fk = schema_foreign_key(
            ['user_id'],
            'users',
            ['id'],
            onDelete: ReferentialAction::CASCADE,
            onUpdate: ReferentialAction::CASCADE,
        );

        static::assertSame(ReferentialAction::CASCADE, $fk->onDelete);
        static::assertSame(ReferentialAction::CASCADE, $fk->onUpdate);
    }

    public function test_is_equal_for_identical_foreign_keys(): void
    {
        $a = schema_foreign_key(
            ['user_id'],
            'users',
            ['id'],
            name: 'fk_orders_user',
            onDelete: ReferentialAction::CASCADE,
        );
        $b = schema_foreign_key(
            ['user_id'],
            'users',
            ['id'],
            name: 'fk_orders_user',
            onDelete: ReferentialAction::CASCADE,
        );

        static::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_columns_differ(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders');
        $b = schema_foreign_key(['account_id'], 'users', ['id'], name: 'fk_orders');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_a');
        $b = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_b');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_on_delete_differs(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders', onDelete: ReferentialAction::CASCADE);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders', onDelete: ReferentialAction::RESTRICT);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_reference_table_differs(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders');
        $b = schema_foreign_key(['user_id'], 'accounts', ['id'], name: 'fk_orders');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_deferrable_differs(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: false);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_initially_deferred_differs(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true, initiallyDeferred: true);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true, initiallyDeferred: false);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_reference_columns_differ(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id']);
        $b = schema_foreign_key(['user_id'], 'users', ['uuid']);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_reference_schema_differs(): void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], referenceSchema: 'public');
        $b = schema_foreign_key(['user_id'], 'users', ['id'], referenceSchema: 'other');

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs(): void
    {
        $a = schema_foreign_key(
            ['user_id'],
            'users',
            ['id'],
            name: 'fk_a',
            onDelete: ReferentialAction::CASCADE,
            deferrable: true,
        );
        $b = schema_foreign_key(
            ['user_id'],
            'users',
            ['id'],
            name: 'fk_b',
            onDelete: ReferentialAction::CASCADE,
            deferrable: true,
        );

        static::assertTrue($a->isEqualStructure($b));
    }
}
