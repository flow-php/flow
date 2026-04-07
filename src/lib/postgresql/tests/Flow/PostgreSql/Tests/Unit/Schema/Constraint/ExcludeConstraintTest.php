<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Constraint;

use function Flow\PostgreSql\DSL\schema_exclude;

use PHPUnit\Framework\TestCase;

final class ExcludeConstraintTest extends TestCase
{
    public function test_exclude_constraint_construction() : void
    {
        $exclude = schema_exclude('USING gist (tsrange WITH &&)');

        self::assertSame('USING gist (tsrange WITH &&)', $exclude->definition);
        self::assertNull($exclude->name);
    }

    public function test_exclude_constraint_with_name() : void
    {
        $exclude = schema_exclude('USING gist (tsrange WITH &&)', 'excl_no_overlap');

        self::assertSame('excl_no_overlap', $exclude->name);
    }

    public function test_is_equal_for_identical_constraints() : void
    {
        $a = schema_exclude('USING gist (tsrange WITH &&)', 'excl_overlap');
        $b = schema_exclude('USING gist (tsrange WITH &&)', 'excl_overlap');

        self::assertTrue($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_definition_differs() : void
    {
        $a = schema_exclude('USING gist (tsrange WITH &&)', 'excl_overlap');
        $b = schema_exclude('USING gist (daterange WITH &&)', 'excl_overlap');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_returns_false_when_name_differs() : void
    {
        $a = schema_exclude('USING gist (tsrange WITH &&)', 'excl_a');
        $b = schema_exclude('USING gist (tsrange WITH &&)', 'excl_b');

        self::assertFalse($a->isEqual($b));
    }

    public function test_is_equal_structure_returns_false_when_definition_differs() : void
    {
        $a = schema_exclude('USING gist (tsrange WITH &&)');
        $b = schema_exclude('USING gist (daterange WITH &&)');

        self::assertFalse($a->isEqualStructure($b));
    }

    public function test_is_equal_structure_returns_true_when_only_name_differs() : void
    {
        $a = schema_exclude('USING gist (tsrange WITH &&)', 'excl_a');
        $b = schema_exclude('USING gist (tsrange WITH &&)', 'excl_b');

        self::assertTrue($a->isEqualStructure($b));
    }
}
