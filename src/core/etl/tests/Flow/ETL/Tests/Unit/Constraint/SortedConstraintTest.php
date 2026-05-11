<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Constraint;

use Flow\ETL\Constraint\SortedByConstraint;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class SortedConstraintTest extends FlowTestCase
{
    public function test_sorted_constraint_as_a_string(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());

        static::assertSame('Sorted constraint on [id ASC]', $constraint->toString());
    }

    public function test_sorted_constraint_ascending_integers(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());

        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 1))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 2))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 3))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 3))));
        static::assertFalse($constraint->isSatisfiedBy(row(int_entry('id', 2))));
    }

    public function test_sorted_constraint_ascending_strings(): void
    {
        $constraint = new SortedByConstraint(ref('name')->asc());

        static::assertTrue($constraint->isSatisfiedBy(row(str_entry('name', 'Alice'))));
        static::assertTrue($constraint->isSatisfiedBy(row(str_entry('name', 'Bob'))));
        static::assertTrue($constraint->isSatisfiedBy(row(str_entry('name', 'Charlie'))));
        static::assertFalse($constraint->isSatisfiedBy(row(str_entry('name', 'Alice'))));
    }

    public function test_sorted_constraint_default_order_is_ascending(): void
    {
        $constraint = new SortedByConstraint(ref('id'));

        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 1))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 2))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 3))));
        static::assertFalse($constraint->isSatisfiedBy(row(int_entry('id', 1))));
    }

    public function test_sorted_constraint_descending_floats(): void
    {
        $constraint = new SortedByConstraint(ref('price')->desc());

        static::assertTrue($constraint->isSatisfiedBy(row(float_entry('price', 99.99))));
        static::assertTrue($constraint->isSatisfiedBy(row(float_entry('price', 49.99))));
        static::assertTrue($constraint->isSatisfiedBy(row(float_entry('price', 19.99))));
        static::assertTrue($constraint->isSatisfiedBy(row(float_entry('price', 19.99))));
        static::assertFalse($constraint->isSatisfiedBy(row(float_entry('price', 29.99))));
    }

    public function test_sorted_constraint_descending_integers(): void
    {
        $constraint = new SortedByConstraint(ref('id')->desc());

        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 10))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 5))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 1))));
        static::assertFalse($constraint->isSatisfiedBy(row(int_entry('id', 3))));
    }

    public function test_sorted_constraint_multiple_columns(): void
    {
        $constraint = new SortedByConstraint(ref('category')->asc(), ref('price')->desc());

        static::assertTrue($constraint->isSatisfiedBy(row(str_entry('category', 'A'), float_entry('price', 100.0))));
        static::assertTrue($constraint->isSatisfiedBy(row(str_entry('category', 'A'), float_entry('price', 50.0))));
        static::assertTrue($constraint->isSatisfiedBy(row(str_entry('category', 'B'), float_entry('price', 200.0))));
        static::assertTrue($constraint->isSatisfiedBy(row(str_entry('category', 'B'), float_entry('price', 150.0))));
        static::assertFalse($constraint->isSatisfiedBy(row(str_entry('category', 'B'), float_entry('price', 250.0))));
    }

    public function test_sorted_constraint_multiple_columns_as_a_string(): void
    {
        $constraint = new SortedByConstraint(ref('category')->asc(), ref('price')->desc(), ref('id')->asc());

        static::assertSame('Sorted constraint on [category ASC, price DESC, id ASC]', $constraint->toString());
    }

    public function test_sorted_constraint_violation_ascending(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());
        $constraint->isSatisfiedBy(row(int_entry('id', 5)));
        $constraint->isSatisfiedBy(row(int_entry('id', 10)));

        $violation = $constraint->violation(row(int_entry('id', 3)));

        static::assertStringContainsString('expected ASC order', $violation);
        static::assertStringContainsString('id<integer>', $violation);
        static::assertStringContainsString('current: 3', $violation);
        static::assertStringContainsString('previous: 10', $violation);
    }

    public function test_sorted_constraint_violation_descending(): void
    {
        $constraint = new SortedByConstraint(ref('id')->desc());
        $constraint->isSatisfiedBy(row(int_entry('id', 10)));
        $constraint->isSatisfiedBy(row(int_entry('id', 5)));

        $violation = $constraint->violation(row(int_entry('id', 8)));

        static::assertStringContainsString('expected DESC order', $violation);
        static::assertStringContainsString('id<integer>', $violation);
        static::assertStringContainsString('current: 8', $violation);
        static::assertStringContainsString('previous: 5', $violation);
    }

    public function test_sorted_constraint_with_dates(): void
    {
        $constraint = new SortedByConstraint(ref('date')->asc());

        static::assertTrue($constraint->isSatisfiedBy(row(date_entry('date', new \DateTimeImmutable('2025-01-01')))));
        static::assertTrue($constraint->isSatisfiedBy(row(date_entry('date', new \DateTimeImmutable('2025-01-02')))));
        static::assertTrue($constraint->isSatisfiedBy(row(date_entry('date', new \DateTimeImmutable('2025-01-03')))));
        static::assertFalse($constraint->isSatisfiedBy(row(date_entry('date', new \DateTimeImmutable('2025-01-01')))));
    }

    public function test_sorted_constraint_with_single_row(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());

        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 42))));
    }
}
