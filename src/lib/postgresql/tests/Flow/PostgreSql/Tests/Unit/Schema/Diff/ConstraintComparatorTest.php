<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use Flow\PostgreSql\Schema\Diff\ConstraintComparator;
use PHPUnit\Framework\TestCase;

final class ConstraintComparatorTest extends TestCase
{
    public function test_check_constraint_added(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints([], [new CheckConstraint('age > 0', 'chk_age')]);

        static::assertCount(1, $result->added);
        static::assertSame('chk_age', $result->added[0]->name);
        static::assertSame([], $result->removed);
    }

    public function test_check_constraint_modified_expression(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints([new CheckConstraint('age > 0', 'chk_age')], [new CheckConstraint(
            'age > 18',
            'chk_age',
        )]);

        static::assertCount(1, $result->added);
        static::assertSame('age > 18', $result->added[0]->expression);
        static::assertCount(1, $result->removed);
    }

    public function test_check_constraint_modified_no_inherit(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints([new CheckConstraint(
            'age > 0',
            'chk_age',
            noInherit: false,
        )], [new CheckConstraint('age > 0', 'chk_age', noInherit: true)]);

        static::assertCount(1, $result->added);
        static::assertTrue($result->added[0]->noInherit);
        static::assertCount(1, $result->removed);
    }

    public function test_check_constraint_removed(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints([new CheckConstraint('age > 0', 'chk_age')], []);

        static::assertSame([], $result->added);
        static::assertCount(1, $result->removed);
    }

    public function test_exclude_constraint_added(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints([], [new ExcludeConstraint(
            'USING gist (range WITH &&)',
            'excl_range',
        )]);

        static::assertCount(1, $result->added);
        static::assertSame('excl_range', $result->added[0]->name);
        static::assertSame([], $result->removed);
    }

    public function test_exclude_constraint_modified_definition(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints([new ExcludeConstraint(
            'USING gist (range WITH &&)',
            'excl_range',
        )], [new ExcludeConstraint('USING gist (period WITH &&)', 'excl_range')]);

        static::assertCount(1, $result->added);
        static::assertSame('USING gist (period WITH &&)', $result->added[0]->definition);
        static::assertCount(1, $result->removed);
    }

    public function test_exclude_constraint_removed(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints([new ExcludeConstraint(
            'USING gist (range WITH &&)',
            'excl_range',
        )], []);

        static::assertSame([], $result->added);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_added(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([], [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])]);

        static::assertCount(1, $result->added);
        static::assertSame('fk_user', $result->added[0]->name);
        static::assertSame([], $result->removed);
    }

    public function test_foreign_key_identical(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
        )], [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
    }

    public function test_foreign_key_modified_columns(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
        )], [new ForeignKey('fk_user', ['account_id'], 'public', 'users', ['id'])]);

        static::assertCount(1, $result->added);
        static::assertSame(['account_id'], $result->added[0]->columns);
        static::assertCount(1, $result->removed);
        static::assertSame(['user_id'], $result->removed[0]->columns);
    }

    public function test_foreign_key_modified_deferrable(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
            deferrable: false,
        )], [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], deferrable: true)]);

        static::assertCount(1, $result->added);
        static::assertTrue($result->added[0]->deferrable);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_initially_deferred(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
            initiallyDeferred: false,
        )], [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], initiallyDeferred: true)]);

        static::assertCount(1, $result->added);
        static::assertTrue($result->added[0]->initiallyDeferred);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_on_delete(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
            onDelete: ReferentialAction::NO_ACTION,
        )], [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], onDelete: ReferentialAction::SET_NULL)]);

        static::assertCount(1, $result->added);
        static::assertSame(ReferentialAction::SET_NULL, $result->added[0]->onDelete);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_on_update(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
            onUpdate: ReferentialAction::NO_ACTION,
        )], [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], onUpdate: ReferentialAction::CASCADE)]);

        static::assertCount(1, $result->added);
        static::assertSame(ReferentialAction::CASCADE, $result->added[0]->onUpdate);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_reference_columns(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
        )], [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['uuid'])]);

        static::assertCount(1, $result->added);
        static::assertSame(['uuid'], $result->added[0]->referenceColumns);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_reference_schema(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
        )], [new ForeignKey('fk_user', ['user_id'], 'audit', 'users', ['id'])]);

        static::assertCount(1, $result->added);
        static::assertSame('audit', $result->added[0]->referenceSchema);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_reference_table(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            'fk_user',
            ['user_id'],
            'public',
            'users',
            ['id'],
        )], [new ForeignKey('fk_user', ['user_id'], 'public', 'accounts', ['id'])]);

        static::assertCount(1, $result->added);
        static::assertSame('accounts', $result->added[0]->referenceTable);
        static::assertCount(1, $result->removed);
    }

    public function test_foreign_key_removed(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])], []);

        static::assertSame([], $result->added);
        static::assertCount(1, $result->removed);
        static::assertSame('fk_user', $result->removed[0]->name);
    }

    public function test_unique_constraint_added(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints([], [new UniqueConstraint(['email'], 'uq_email')]);

        static::assertCount(1, $result->added);
        static::assertSame('uq_email', $result->added[0]->name);
        static::assertSame([], $result->removed);
    }

    public function test_unique_constraint_identical_columns_in_different_order(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints([new UniqueConstraint(['a', 'b'], 'uq_ab')], [new UniqueConstraint(
            ['b', 'a'],
            'uq_ab',
        )]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
    }

    public function test_unique_constraint_modified_columns(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints([new UniqueConstraint(
            ['email'],
            'uq_email',
        )], [new UniqueConstraint(['username'], 'uq_email')]);

        static::assertCount(1, $result->added);
        static::assertSame(['username'], $result->added[0]->columns);
        static::assertCount(1, $result->removed);
    }

    public function test_unique_constraint_modified_nulls_not_distinct(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints([new UniqueConstraint(
            ['email'],
            'uq_email',
            nullsNotDistinct: false,
        )], [new UniqueConstraint(['email'], 'uq_email', nullsNotDistinct: true)]);

        static::assertCount(1, $result->added);
        static::assertTrue($result->added[0]->nullsNotDistinct);
        static::assertCount(1, $result->removed);
    }

    public function test_unique_constraint_removed(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints([new UniqueConstraint(['email'], 'uq_email')], []);

        static::assertSame([], $result->added);
        static::assertCount(1, $result->removed);
    }

    public function test_unnamed_check_constraint_matched_by_expression(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints([new CheckConstraint('age > 0')], [new CheckConstraint('age > 0')]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
    }

    public function test_unnamed_exclude_constraint_matched_by_definition(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints([new ExcludeConstraint(
            'USING gist (range WITH &&)',
        )], [new ExcludeConstraint('USING gist (range WITH &&)')]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
    }

    public function test_unnamed_foreign_key_matched_by_structural_identity(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys([new ForeignKey(
            null,
            ['user_id'],
            'public',
            'users',
            ['id'],
        )], [new ForeignKey(null, ['user_id'], 'public', 'users', ['id'])]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
    }

    public function test_unnamed_unique_constraint_matched_by_columns(): void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints([new UniqueConstraint([
            'email',
            'tenant_id',
        ])], [new UniqueConstraint(['tenant_id', 'email'])]);

        static::assertSame([], $result->added);
        static::assertSame([], $result->removed);
    }
}
