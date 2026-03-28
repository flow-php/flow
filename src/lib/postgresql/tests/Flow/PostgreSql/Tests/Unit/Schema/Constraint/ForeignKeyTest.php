<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use function Flow\PostgreSql\DSL\schema_foreign_key;
use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;

use PHPUnit\Framework\TestCase;

final class ForeignKeyTest extends TestCase
{
    public function test_foreign_key_construction() : void
    {
        $fk = schema_foreign_key(['user_id'], 'users', ['id']);

        self::assertSame(['user_id'], $fk->columns);
        self::assertSame('users', $fk->referenceTable);
        self::assertSame(['id'], $fk->referenceColumns);
        self::assertSame('public', $fk->referenceSchema);
        self::assertNull($fk->name);
    }

    public function test_foreign_key_deferrable() : void
    {
        $fk = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true, initiallyDeferred: true);

        self::assertTrue($fk->deferrable);
        self::assertTrue($fk->initiallyDeferred);
    }

    public function test_foreign_key_with_actions() : void
    {
        $fk = schema_foreign_key(['user_id'], 'users', ['id'], onDelete: ReferentialAction::CASCADE, onUpdate: ReferentialAction::CASCADE);

        self::assertSame(ReferentialAction::CASCADE, $fk->onDelete);
        self::assertSame(ReferentialAction::CASCADE, $fk->onUpdate);
    }

    public function test_is_equal_for_identical_foreign_keys() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders_user', onDelete: ReferentialAction::CASCADE);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders_user', onDelete: ReferentialAction::CASCADE);

        self::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_columns_differ() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders');
        $b = schema_foreign_key(['account_id'], 'users', ['id'], name: 'fk_orders');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_a');
        $b = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_b');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_on_delete_differs() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders', onDelete: ReferentialAction::CASCADE);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders', onDelete: ReferentialAction::RESTRICT);

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_reference_table_differs() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_orders');
        $b = schema_foreign_key(['user_id'], 'accounts', ['id'], name: 'fk_orders');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_deferrable_differs() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: false);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_initially_deferred_differs() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true, initiallyDeferred: true);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], deferrable: true, initiallyDeferred: false);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_reference_columns_differ() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id']);
        $b = schema_foreign_key(['user_id'], 'users', ['uuid']);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_reference_schema_differs() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], referenceSchema: 'public');
        $b = schema_foreign_key(['user_id'], 'users', ['id'], referenceSchema: 'other');

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs() : void
    {
        $a = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_a', onDelete: ReferentialAction::CASCADE, deferrable: true);
        $b = schema_foreign_key(['user_id'], 'users', ['id'], name: 'fk_b', onDelete: ReferentialAction::CASCADE, deferrable: true);

        self::assertTrue($a->isEqualStructure($b));
    }
}
