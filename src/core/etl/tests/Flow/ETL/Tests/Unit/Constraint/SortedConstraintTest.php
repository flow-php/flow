<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Constraint;

use DateTimeImmutable;
use Flow\ETL\Constraint\SortedByConstraint;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

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

        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 1]), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 2]), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 3]), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 3]), schema(int_schema('id'))));
        static::assertFalse($constraint->isSatisfiedBy(row(['id' => 2]), schema(int_schema('id'))));
    }

    public function test_sorted_constraint_ascending_strings(): void
    {
        $constraint = new SortedByConstraint(ref('name')->asc());

        static::assertTrue($constraint->isSatisfiedBy(row(['name' => 'Alice']), schema(str_schema('name'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['name' => 'Bob']), schema(str_schema('name'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['name' => 'Charlie']), schema(str_schema('name'))));
        static::assertFalse($constraint->isSatisfiedBy(row(['name' => 'Alice']), schema(str_schema('name'))));
    }

    public function test_sorted_constraint_default_order_is_ascending(): void
    {
        $constraint = new SortedByConstraint(ref('id'));

        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 1]), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 2]), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 3]), schema(int_schema('id'))));
        static::assertFalse($constraint->isSatisfiedBy(row(['id' => 1]), schema(int_schema('id'))));
    }

    public function test_sorted_constraint_descending_floats(): void
    {
        $constraint = new SortedByConstraint(ref('price')->desc());

        static::assertTrue($constraint->isSatisfiedBy(row(['price' => 99.99]), schema(float_schema('price'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['price' => 49.99]), schema(float_schema('price'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['price' => 19.99]), schema(float_schema('price'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['price' => 19.99]), schema(float_schema('price'))));
        static::assertFalse($constraint->isSatisfiedBy(row(['price' => 29.99]), schema(float_schema('price'))));
    }

    public function test_sorted_constraint_descending_integers(): void
    {
        $constraint = new SortedByConstraint(ref('id')->desc());

        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 10]), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 5]), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 1]), schema(int_schema('id'))));
        static::assertFalse($constraint->isSatisfiedBy(row(['id' => 3]), schema(int_schema('id'))));
    }

    public function test_sorted_constraint_multiple_columns(): void
    {
        $constraint = new SortedByConstraint(ref('category')->asc(), ref('price')->desc());

        static::assertTrue($constraint->isSatisfiedBy(
            row(['category' => 'A', 'price' => 100.0]),
            schema(str_schema('category'), float_schema('price')),
        ));
        static::assertTrue($constraint->isSatisfiedBy(
            row(['category' => 'A', 'price' => 50.0]),
            schema(str_schema('category'), float_schema('price')),
        ));
        static::assertTrue($constraint->isSatisfiedBy(
            row(['category' => 'B', 'price' => 200.0]),
            schema(str_schema('category'), float_schema('price')),
        ));
        static::assertTrue($constraint->isSatisfiedBy(
            row(['category' => 'B', 'price' => 150.0]),
            schema(str_schema('category'), float_schema('price')),
        ));
        static::assertFalse($constraint->isSatisfiedBy(
            row(['category' => 'B', 'price' => 250.0]),
            schema(str_schema('category'), float_schema('price')),
        ));
    }

    public function test_sorted_constraint_multiple_columns_as_a_string(): void
    {
        $constraint = new SortedByConstraint(ref('category')->asc(), ref('price')->desc(), ref('id')->asc());

        static::assertSame('Sorted constraint on [category ASC, price DESC, id ASC]', $constraint->toString());
    }

    public function test_sorted_constraint_violation_ascending(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());
        $constraint->isSatisfiedBy(row(['id' => 5]), schema(int_schema('id')));
        $constraint->isSatisfiedBy(row(['id' => 10]), schema(int_schema('id')));

        $violation = $constraint->violation(row(['id' => 3]), schema(int_schema('id')));

        static::assertStringContainsString('expected ASC order', $violation);
        static::assertStringContainsString('id<integer>', $violation);
        static::assertStringContainsString('current: 3', $violation);
        static::assertStringContainsString('previous: 10', $violation);
    }

    public function test_sorted_constraint_violation_descending(): void
    {
        $constraint = new SortedByConstraint(ref('id')->desc());
        $constraint->isSatisfiedBy(row(['id' => 10]), schema(int_schema('id')));
        $constraint->isSatisfiedBy(row(['id' => 5]), schema(int_schema('id')));

        $violation = $constraint->violation(row(['id' => 8]), schema(int_schema('id')));

        static::assertStringContainsString('expected DESC order', $violation);
        static::assertStringContainsString('id<integer>', $violation);
        static::assertStringContainsString('current: 8', $violation);
        static::assertStringContainsString('previous: 5', $violation);
    }

    public function test_sorted_constraint_with_dates(): void
    {
        $constraint = new SortedByConstraint(ref('date')->asc());

        static::assertTrue($constraint->isSatisfiedBy(row([
            'date' => new DateTimeImmutable('2025-01-01'),
        ]), schema(date_schema('date'))));
        static::assertTrue($constraint->isSatisfiedBy(row([
            'date' => new DateTimeImmutable('2025-01-02'),
        ]), schema(date_schema('date'))));
        static::assertTrue($constraint->isSatisfiedBy(row([
            'date' => new DateTimeImmutable('2025-01-03'),
        ]), schema(date_schema('date'))));
        static::assertFalse($constraint->isSatisfiedBy(row([
            'date' => new DateTimeImmutable('2025-01-01'),
        ]), schema(date_schema('date'))));
    }

    public function test_sorted_constraint_with_single_row(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());

        static::assertTrue($constraint->isSatisfiedBy(row(['id' => 42]), schema(int_schema('id'))));
    }
}
