<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Constraint;

use DateTimeImmutable;
use Flow\ETL\Constraint\SortedByConstraint;
use Flow\ETL\Row\NullsOrder;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

use const NAN;

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

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 2,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 3,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 3,
        ]], schema(int_schema('id')))));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows([[
                'id' => 2,
            ]], schema(int_schema('id')))),
        );
    }

    public function test_sorted_constraint_ascending_strings(): void
    {
        $constraint = new SortedByConstraint(ref('name')->asc());

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'name' => 'Alice',
        ]], schema(str_schema('name')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'name' => 'Bob',
        ]], schema(str_schema('name')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'name' => 'Charlie',
        ]], schema(str_schema('name')))));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows([[
                'name' => 'Alice',
            ]], schema(str_schema('name')))),
        );
    }

    public function test_sorted_constraint_default_order_is_ascending(): void
    {
        $constraint = new SortedByConstraint(ref('id'));

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 2,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 3,
        ]], schema(int_schema('id')))));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows([[
                'id' => 1,
            ]], schema(int_schema('id')))),
        );
    }

    public function test_sorted_constraint_descending_floats(): void
    {
        $constraint = new SortedByConstraint(ref('price')->desc());

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'price' => 99.99,
        ]], schema(float_schema('price')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'price' => 49.99,
        ]], schema(float_schema('price')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'price' => 19.99,
        ]], schema(float_schema('price')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'price' => 19.99,
        ]], schema(float_schema('price')))));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows([[
                'price' => 29.99,
            ]], schema(float_schema('price')))),
        );
    }

    public function test_sorted_constraint_descending_integers(): void
    {
        $constraint = new SortedByConstraint(ref('id')->desc());

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 10,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 5,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows([[
                'id' => 3,
            ]], schema(int_schema('id')))),
        );
    }

    public function test_sorted_constraint_multiple_columns(): void
    {
        $constraint = new SortedByConstraint(ref('category')->asc(), ref('price')->desc());

        static::assertNull($constraint->firstViolation(array_to_rows(
            [['category' => 'A', 'price' => 100.0]],
            schema(str_schema('category'), float_schema('price')),
        )));
        static::assertNull($constraint->firstViolation(array_to_rows(
            [['category' => 'A', 'price' => 50.0]],
            schema(str_schema('category'), float_schema('price')),
        )));
        static::assertNull($constraint->firstViolation(array_to_rows(
            [['category' => 'B', 'price' => 200.0]],
            schema(str_schema('category'), float_schema('price')),
        )));
        static::assertNull($constraint->firstViolation(array_to_rows(
            [['category' => 'B', 'price' => 150.0]],
            schema(str_schema('category'), float_schema('price')),
        )));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows(
                [['category' => 'B', 'price' => 250.0]],
                schema(str_schema('category'), float_schema('price')),
            )),
        );
    }

    public function test_sorted_constraint_multiple_columns_as_a_string(): void
    {
        $constraint = new SortedByConstraint(ref('category')->asc(), ref('price')->desc(), ref('id')->asc());

        static::assertSame('Sorted constraint on [category ASC, price DESC, id ASC]', $constraint->toString());
    }

    public function test_sorted_constraint_violation_ascending(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());
        $constraint->firstViolation(array_to_rows([['id' => 5]], schema(int_schema('id'))));
        $constraint->firstViolation(array_to_rows([['id' => 10]], schema(int_schema('id'))));

        $violation = $constraint->violation(array_to_rows([[
            'id' => 3,
        ]], schema(int_schema('id'))), 0);

        static::assertStringContainsString('expected ASC order', $violation);
        static::assertStringContainsString('id<integer>', $violation);
        static::assertStringContainsString('current: 3', $violation);
        static::assertStringContainsString('previous: 10', $violation);
    }

    public function test_sorted_constraint_violation_descending(): void
    {
        $constraint = new SortedByConstraint(ref('id')->desc());
        $constraint->firstViolation(array_to_rows([['id' => 10]], schema(int_schema('id'))));
        $constraint->firstViolation(array_to_rows([['id' => 5]], schema(int_schema('id'))));

        $violation = $constraint->violation(array_to_rows([[
            'id' => 8,
        ]], schema(int_schema('id'))), 0);

        static::assertStringContainsString('expected DESC order', $violation);
        static::assertStringContainsString('id<integer>', $violation);
        static::assertStringContainsString('current: 8', $violation);
        static::assertStringContainsString('previous: 5', $violation);
    }

    public function test_sorted_constraint_with_dates(): void
    {
        $constraint = new SortedByConstraint(ref('date')->asc());

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'date' => new DateTimeImmutable('2025-01-01'),
        ]], schema(datetime_schema('date')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'date' => new DateTimeImmutable('2025-01-02'),
        ]], schema(datetime_schema('date')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'date' => new DateTimeImmutable('2025-01-03'),
        ]], schema(datetime_schema('date')))));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows([[
                'date' => new DateTimeImmutable('2025-01-01'),
            ]], schema(datetime_schema('date')))),
        );
    }

    public function test_sorted_constraint_with_single_row(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 42,
        ]], schema(int_schema('id')))));
    }

    public function test_first_violation_carries_the_previous_batch_last_row(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());

        static::assertNull($constraint->firstViolation(array_to_rows([
            ['id' => 1],
            ['id' => 5],
        ], schema(int_schema('id')))));

        $batch = array_to_rows([['id' => 4], ['id' => 6]], schema(int_schema('id')));

        static::assertSame(0, $constraint->firstViolation($batch));
        static::assertStringContainsString('current: 4, previous: 5', $constraint->violation($batch, 0));
    }

    public function test_first_violation_stops_advancing_at_the_violation(): void
    {
        $constraint = new SortedByConstraint(ref('id')->asc());
        $batch = array_to_rows([['id' => 1], ['id' => 3], ['id' => 2], ['id' => 9]], schema(int_schema('id')));

        static::assertSame(2, $constraint->firstViolation($batch));
        static::assertStringContainsString('current: 2, previous: 3', $constraint->violation($batch, 2));
    }

    public function test_an_empty_batch_has_no_violation(): void
    {
        static::assertNull((new SortedByConstraint(ref('id')))->firstViolation(array_to_rows(
            [],
            schema(int_schema('id')),
        )));
    }

    public function test_datetimes_are_compared_by_instant(): void
    {
        $constraint = new SortedByConstraint(ref('at')->asc());

        static::assertSame(
            2,
            $constraint->firstViolation(array_to_rows([
                ['at' => new DateTimeImmutable('2024-01-01 00:00:00.000001')],
                ['at' => new DateTimeImmutable('2024-01-01 00:00:00.000002')],
                ['at' => new DateTimeImmutable('2024-01-01 00:00:00.000001')],
            ], schema(datetime_schema('at')))),
        );
    }

    public function test_strings_are_in_byte_order_never_numeric(): void
    {
        static::assertSame(
            1,
            (new SortedByConstraint(ref('code')->asc()))->firstViolation(array_to_rows([
                ['code' => '9'],
                ['code' => '10'],
            ], schema(str_schema('code')))),
        );
        static::assertNull((new SortedByConstraint(ref('code')->asc()))->firstViolation(array_to_rows([
            ['code' => '10'],
            ['code' => '9'],
        ], schema(str_schema('code')))));
    }

    public function test_nulls_come_first_ascending_and_nan_last(): void
    {
        static::assertNull((new SortedByConstraint(ref('v')->asc()))->firstViolation(array_to_rows([
            ['v' => null],
            ['v' => -1.5],
            ['v' => 2.0],
            ['v' => NAN],
            ['v' => NAN],
        ], schema(float_schema('v', true)))));
        static::assertSame(
            1,
            (new SortedByConstraint(ref('v')->desc()))->firstViolation(array_to_rows([
                ['v' => null],
                ['v' => NAN],
            ], schema(float_schema('v', true)))),
        );
    }

    public function test_a_moved_null_placement_is_part_of_the_order(): void
    {
        $constraint = new SortedByConstraint(ref('v')->asc(NullsOrder::LAST));

        static::assertSame('Sorted constraint on [v ASC NULLS LAST]', $constraint->toString());
        static::assertNull($constraint->firstViolation(array_to_rows([
            ['v' => 1],
            ['v' => 2],
            ['v' => null],
        ], schema(int_schema('v', true)))));
        static::assertSame(
            1,
            (new SortedByConstraint(ref('v')->asc(NullsOrder::LAST)))->firstViolation(array_to_rows([
                ['v' => null],
                ['v' => 1],
            ], schema(int_schema('v', true)))),
        );
    }
}
