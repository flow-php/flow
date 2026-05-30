<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_unique;

final class UniqueConstraintTest extends TestCase
{
    public function test_is_equal_for_identical_constraints(): void
    {
        $a = schema_unique(['email'], 'uq_email', nullsNotDistinct: true);
        $b = schema_unique(['email'], 'uq_email', nullsNotDistinct: true);

        static::assertTrue($a->isEqual($b));
    }

    public function test_from_array_defaults_nulls_not_distinct_when_absent(): void
    {
        $unique = UniqueConstraint::fromArray([
            'columns' => ['email'],
            'name' => 'uq_email',
        ]);

        static::assertSame(['email'], $unique->columns);
        static::assertSame('uq_email', $unique->name);
        static::assertFalse($unique->nullsNotDistinct);
    }

    public function test_from_array_defaults_name_when_absent(): void
    {
        $unique = UniqueConstraint::fromArray([
            'columns' => ['email'],
        ]);

        static::assertNull($unique->name);
        static::assertFalse($unique->nullsNotDistinct);
    }

    public function test_from_array_with_all_keys(): void
    {
        $unique = UniqueConstraint::fromArray([
            'columns' => ['email', 'tenant_id'],
            'name' => 'uq_email_tenant',
            'nulls_not_distinct' => true,
        ]);

        static::assertSame(['email', 'tenant_id'], $unique->columns);
        static::assertSame('uq_email_tenant', $unique->name);
        static::assertTrue($unique->nullsNotDistinct);
    }

    public function test_normalize_and_from_array_round_trip(): void
    {
        $unique = schema_unique(['email', 'tenant_id'], 'uq_email_tenant', nullsNotDistinct: true);

        static::assertTrue($unique->isEqual(UniqueConstraint::fromArray($unique->normalize())));
    }

    public function test_is_equal_returns_false_when_columns_differ(): void
    {
        $a = schema_unique(['email'], 'uq_users');
        $b = schema_unique(['username'], 'uq_users');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs(): void
    {
        $a = schema_unique(['email'], 'uq_a');
        $b = schema_unique(['email'], 'uq_b');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_nulls_not_distinct_differs(): void
    {
        $a = schema_unique(['email'], 'uq_email', nullsNotDistinct: true);
        $b = schema_unique(['email'], 'uq_email', nullsNotDistinct: false);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_columns_differ(): void
    {
        $a = schema_unique(['email']);
        $b = schema_unique(['username']);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_nulls_not_distinct_differs(): void
    {
        $a = schema_unique(['email'], nullsNotDistinct: true);
        $b = schema_unique(['email'], nullsNotDistinct: false);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_for_columns_in_different_order(): void
    {
        $a = schema_unique(['email', 'tenant_id'], 'uq_users');
        $b = schema_unique(['tenant_id', 'email'], 'uq_users');

        static::assertTrue($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs(): void
    {
        $a = schema_unique(['email', 'tenant_id'], 'uq_a', nullsNotDistinct: true);
        $b = schema_unique(['email', 'tenant_id'], 'uq_b', nullsNotDistinct: true);

        static::assertTrue($a->isEqualStructure($b));
    }

    public function test_unique_constraint_construction(): void
    {
        $unique = schema_unique(['email']);

        static::assertSame(['email'], $unique->columns);
        static::assertNull($unique->name);
        static::assertFalse($unique->nullsNotDistinct);
    }

    public function test_unique_constraint_nulls_not_distinct(): void
    {
        $unique = schema_unique(['email'], nullsNotDistinct: true);

        static::assertTrue($unique->nullsNotDistinct);
    }

    public function test_unique_constraint_with_name(): void
    {
        $unique = schema_unique(['email', 'tenant_id'], 'uq_users_email_tenant');

        static::assertSame(['email', 'tenant_id'], $unique->columns);
        static::assertSame('uq_users_email_tenant', $unique->name);
    }
}
