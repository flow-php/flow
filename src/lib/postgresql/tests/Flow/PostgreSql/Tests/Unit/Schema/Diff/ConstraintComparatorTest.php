<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Schema\ReferentialAction;
use Flow\PostgreSql\Schema\Constraint\{CheckConstraint, ExcludeConstraint, ForeignKey, UniqueConstraint};
use Flow\PostgreSql\Schema\Diff\ConstraintComparator;
use PHPUnit\Framework\TestCase;

final class ConstraintComparatorTest extends TestCase
{
    public function test_check_constraint_added() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints(
            [],
            [new CheckConstraint('age > 0', 'chk_age')],
        );

        self::assertCount(1, $result->added);
        self::assertSame('chk_age', $result->added[0]->name);
        self::assertSame([], $result->removed);
    }

    public function test_check_constraint_modified_expression() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints(
            [new CheckConstraint('age > 0', 'chk_age')],
            [new CheckConstraint('age > 18', 'chk_age')],
        );

        self::assertCount(1, $result->added);
        self::assertSame('age > 18', $result->added[0]->expression);
        self::assertCount(1, $result->removed);
    }

    public function test_check_constraint_modified_no_inherit() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints(
            [new CheckConstraint('age > 0', 'chk_age', noInherit: false)],
            [new CheckConstraint('age > 0', 'chk_age', noInherit: true)],
        );

        self::assertCount(1, $result->added);
        self::assertTrue($result->added[0]->noInherit);
        self::assertCount(1, $result->removed);
    }

    public function test_check_constraint_removed() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints(
            [new CheckConstraint('age > 0', 'chk_age')],
            [],
        );

        self::assertSame([], $result->added);
        self::assertCount(1, $result->removed);
    }

    public function test_exclude_constraint_added() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints(
            [],
            [new ExcludeConstraint('USING gist (range WITH &&)', 'excl_range')],
        );

        self::assertCount(1, $result->added);
        self::assertSame('excl_range', $result->added[0]->name);
        self::assertSame([], $result->removed);
    }

    public function test_exclude_constraint_modified_definition() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints(
            [new ExcludeConstraint('USING gist (range WITH &&)', 'excl_range')],
            [new ExcludeConstraint('USING gist (period WITH &&)', 'excl_range')],
        );

        self::assertCount(1, $result->added);
        self::assertSame('USING gist (period WITH &&)', $result->added[0]->definition);
        self::assertCount(1, $result->removed);
    }

    public function test_exclude_constraint_removed() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints(
            [new ExcludeConstraint('USING gist (range WITH &&)', 'excl_range')],
            [],
        );

        self::assertSame([], $result->added);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_added() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
        );

        self::assertCount(1, $result->added);
        self::assertSame('fk_user', $result->added[0]->name);
        self::assertSame([], $result->removed);
    }

    public function test_foreign_key_identical() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
    }

    public function test_foreign_key_modified_columns() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
            [new ForeignKey('fk_user', ['account_id'], 'public', 'users', ['id'])],
        );

        self::assertCount(1, $result->added);
        self::assertSame(['account_id'], $result->added[0]->columns);
        self::assertCount(1, $result->removed);
        self::assertSame(['user_id'], $result->removed[0]->columns);
    }

    public function test_foreign_key_modified_deferrable() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], deferrable: false)],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], deferrable: true)],
        );

        self::assertCount(1, $result->added);
        self::assertTrue($result->added[0]->deferrable);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_initially_deferred() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], initiallyDeferred: false)],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], initiallyDeferred: true)],
        );

        self::assertCount(1, $result->added);
        self::assertTrue($result->added[0]->initiallyDeferred);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_on_delete() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], onDelete: ReferentialAction::NO_ACTION)],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], onDelete: ReferentialAction::SET_NULL)],
        );

        self::assertCount(1, $result->added);
        self::assertSame(ReferentialAction::SET_NULL, $result->added[0]->onDelete);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_on_update() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], onUpdate: ReferentialAction::NO_ACTION)],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'], onUpdate: ReferentialAction::CASCADE)],
        );

        self::assertCount(1, $result->added);
        self::assertSame(ReferentialAction::CASCADE, $result->added[0]->onUpdate);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_reference_columns() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['uuid'])],
        );

        self::assertCount(1, $result->added);
        self::assertSame(['uuid'], $result->added[0]->referenceColumns);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_reference_schema() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
            [new ForeignKey('fk_user', ['user_id'], 'audit', 'users', ['id'])],
        );

        self::assertCount(1, $result->added);
        self::assertSame('audit', $result->added[0]->referenceSchema);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_modified_reference_table() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
            [new ForeignKey('fk_user', ['user_id'], 'public', 'accounts', ['id'])],
        );

        self::assertCount(1, $result->added);
        self::assertSame('accounts', $result->added[0]->referenceTable);
        self::assertCount(1, $result->removed);
    }

    public function test_foreign_key_removed() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey('fk_user', ['user_id'], 'public', 'users', ['id'])],
            [],
        );

        self::assertSame([], $result->added);
        self::assertCount(1, $result->removed);
        self::assertSame('fk_user', $result->removed[0]->name);
    }

    public function test_unique_constraint_added() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints(
            [],
            [new UniqueConstraint(['email'], 'uq_email')],
        );

        self::assertCount(1, $result->added);
        self::assertSame('uq_email', $result->added[0]->name);
        self::assertSame([], $result->removed);
    }

    public function test_unique_constraint_identical_columns_in_different_order() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints(
            [new UniqueConstraint(['a', 'b'], 'uq_ab')],
            [new UniqueConstraint(['b', 'a'], 'uq_ab')],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
    }

    public function test_unique_constraint_modified_columns() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints(
            [new UniqueConstraint(['email'], 'uq_email')],
            [new UniqueConstraint(['username'], 'uq_email')],
        );

        self::assertCount(1, $result->added);
        self::assertSame(['username'], $result->added[0]->columns);
        self::assertCount(1, $result->removed);
    }

    public function test_unique_constraint_modified_nulls_not_distinct() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints(
            [new UniqueConstraint(['email'], 'uq_email', nullsNotDistinct: false)],
            [new UniqueConstraint(['email'], 'uq_email', nullsNotDistinct: true)],
        );

        self::assertCount(1, $result->added);
        self::assertTrue($result->added[0]->nullsNotDistinct);
        self::assertCount(1, $result->removed);
    }

    public function test_unique_constraint_removed() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints(
            [new UniqueConstraint(['email'], 'uq_email')],
            [],
        );

        self::assertSame([], $result->added);
        self::assertCount(1, $result->removed);
    }

    public function test_unnamed_check_constraint_matched_by_expression() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffCheckConstraints(
            [new CheckConstraint('age > 0')],
            [new CheckConstraint('age > 0')],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
    }

    public function test_unnamed_exclude_constraint_matched_by_definition() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffExcludeConstraints(
            [new ExcludeConstraint('USING gist (range WITH &&)')],
            [new ExcludeConstraint('USING gist (range WITH &&)')],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
    }

    public function test_unnamed_foreign_key_matched_by_structural_identity() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffForeignKeys(
            [new ForeignKey(null, ['user_id'], 'public', 'users', ['id'])],
            [new ForeignKey(null, ['user_id'], 'public', 'users', ['id'])],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
    }

    public function test_unnamed_unique_constraint_matched_by_columns() : void
    {
        $comparator = new ConstraintComparator();

        $result = $comparator->diffUniqueConstraints(
            [new UniqueConstraint(['email', 'tenant_id'])],
            [new UniqueConstraint(['tenant_id', 'email'])],
        );

        self::assertSame([], $result->added);
        self::assertSame([], $result->removed);
    }
}
