<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use function Flow\PostgreSql\DSL\schema_check;

use PHPUnit\Framework\TestCase;

final class CheckConstraintTest extends TestCase
{
    public function test_check_constraint_construction() : void
    {
        $check = schema_check('age > 0');

        self::assertSame('age > 0', $check->expression);
        self::assertNull($check->name);
        self::assertFalse($check->noInherit);
    }

    public function test_check_constraint_no_inherit() : void
    {
        $check = schema_check('age > 0', noInherit: true);

        self::assertTrue($check->noInherit);
    }

    public function test_check_constraint_with_name() : void
    {
        $check = schema_check('age > 0', 'chk_positive_age');

        self::assertSame('chk_positive_age', $check->name);
    }

    public function test_is_equal_for_identical_constraints() : void
    {
        $a = schema_check('age > 0', 'chk_age', noInherit: true);
        $b = schema_check('age > 0', 'chk_age', noInherit: true);

        self::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_expression_differs() : void
    {
        $a = schema_check('age > 0', 'chk_age');
        $b = schema_check('age >= 0', 'chk_age');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs() : void
    {
        $a = schema_check('age > 0', 'chk_a');
        $b = schema_check('age > 0', 'chk_b');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_no_inherit_differs() : void
    {
        $a = schema_check('age > 0', 'chk_age', noInherit: true);
        $b = schema_check('age > 0', 'chk_age', noInherit: false);

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_expression_differs() : void
    {
        $a = schema_check('age > 0');
        $b = schema_check('age >= 18');

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_false_when_no_inherit_differs() : void
    {
        $a = schema_check('age > 0', noInherit: true);
        $b = schema_check('age > 0', noInherit: false);

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs() : void
    {
        $a = schema_check('age > 0', 'chk_a', noInherit: true);
        $b = schema_check('age > 0', 'chk_b', noInherit: true);

        self::assertTrue($a->isEqualStructure($b));
    }
}
