<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\schema_check;

final class CheckConstraintTest extends TestCase
{
    public function test_check_constraint_construction(): void
    {
        $check = schema_check('age > 0');

        static::assertSame('age > 0', $check->expression);
        static::assertNull($check->name);
        static::assertFalse($check->noInherit);
    }

    public function test_check_constraint_no_inherit(): void
    {
        $check = schema_check('age > 0', noInherit: true);

        static::assertTrue($check->noInherit);
    }

    public function test_check_constraint_with_name(): void
    {
        $check = schema_check('age > 0', 'chk_positive_age');

        static::assertSame('chk_positive_age', $check->name);
    }

    public function test_is_equal_for_identical_constraints(): void
    {
        $a = schema_check('age > 0', 'chk_age', noInherit: true);
        $b = schema_check('age > 0', 'chk_age', noInherit: true);

        static::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_expression_differs(): void
    {
        $a = schema_check('age > 0', 'chk_age');
        $b = schema_check('age >= 0', 'chk_age');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs(): void
    {
        $a = schema_check('age > 0', 'chk_a');
        $b = schema_check('age > 0', 'chk_b');

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_no_inherit_differs(): void
    {
        $a = schema_check('age > 0', 'chk_age', noInherit: true);
        $b = schema_check('age > 0', 'chk_age', noInherit: false);

        static::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_expression_differs(): void
    {
        $a = schema_check('age > 0');
        $b = schema_check('age >= 18');

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_no_inherit_differs(): void
    {
        $a = schema_check('age > 0', noInherit: true);
        $b = schema_check('age > 0', noInherit: false);

        static::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs(): void
    {
        $a = schema_check('age > 0', 'chk_a', noInherit: true);
        $b = schema_check('age > 0', 'chk_b', noInherit: true);

        static::assertTrue($a->isEqualStructure($b));
    }
}
