<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use function Flow\PostgreSql\DSL\schema_unique;

use PHPUnit\Framework\TestCase;

final class UniqueConstraintTest extends TestCase
{
    public function test_is_equal_for_identical_constraints() : void
    {
        $a = schema_unique(['email'], 'uq_email', nullsNotDistinct: true);
        $b = schema_unique(['email'], 'uq_email', nullsNotDistinct: true);

        self::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_columns_differ() : void
    {
        $a = schema_unique(['email'], 'uq_users');
        $b = schema_unique(['username'], 'uq_users');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs() : void
    {
        $a = schema_unique(['email'], 'uq_a');
        $b = schema_unique(['email'], 'uq_b');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_nulls_not_distinct_differs() : void
    {
        $a = schema_unique(['email'], 'uq_email', nullsNotDistinct: true);
        $b = schema_unique(['email'], 'uq_email', nullsNotDistinct: false);

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_columns_differ() : void
    {
        $a = schema_unique(['email']);
        $b = schema_unique(['username']);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_nulls_not_distinct_differs() : void
    {
        $a = schema_unique(['email'], nullsNotDistinct: true);
        $b = schema_unique(['email'], nullsNotDistinct: false);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_for_columns_in_different_order() : void
    {
        $a = schema_unique(['email', 'tenant_id'], 'uq_users');
        $b = schema_unique(['tenant_id', 'email'], 'uq_users');

        self::assertTrue($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs() : void
    {
        $a = schema_unique(['email', 'tenant_id'], 'uq_a', nullsNotDistinct: true);
        $b = schema_unique(['email', 'tenant_id'], 'uq_b', nullsNotDistinct: true);

        self::assertTrue($a->isEqualStructure($b));
    }

    public function test_unique_constraint_construction() : void
    {
        $unique = schema_unique(['email']);

        self::assertSame(['email'], $unique->columns);
        self::assertNull($unique->name);
        self::assertFalse($unique->nullsNotDistinct);
    }

    public function test_unique_constraint_nulls_not_distinct() : void
    {
        $unique = schema_unique(['email'], nullsNotDistinct: true);

        self::assertTrue($unique->nullsNotDistinct);
    }

    public function test_unique_constraint_with_name() : void
    {
        $unique = schema_unique(['email', 'tenant_id'], 'uq_users_email_tenant');

        self::assertSame(['email', 'tenant_id'], $unique->columns);
        self::assertSame('uq_users_email_tenant', $unique->name);
    }
}
