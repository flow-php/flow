<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Constraint;

use DateTimeImmutable;
use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class UniqueConstraintTest extends FlowTestCase
{
    public function test_unique_constraint_as_a_string(): void
    {
        $constraint = new UniqueConstraint('id', 'sub_id');

        static::assertSame('Unique constraint on [id, sub_id]', $constraint->toString());
    }

    public function test_unique_constraint_on_multiple_columns(): void
    {
        $constraint = new UniqueConstraint('id', 'sub_id');

        static::assertNull($constraint->firstViolation(array_to_rows(
            [['id' => 1, 'sub_id' => 1]],
            schema(int_schema('id'), int_schema('sub_id')),
        )));
        static::assertNull($constraint->firstViolation(array_to_rows(
            [['id' => 1, 'sub_id' => 2]],
            schema(int_schema('id'), int_schema('sub_id')),
        )));
        static::assertNull($constraint->firstViolation(array_to_rows(
            [['id' => 2, 'sub_id' => 1]],
            schema(int_schema('id'), int_schema('sub_id')),
        )));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows(
                [['id' => 1, 'sub_id' => 1]],
                schema(int_schema('id'), int_schema('sub_id')),
            )),
        );
    }

    public function test_unique_constraint_on_single_column(): void
    {
        $constraint = new UniqueConstraint('id');

        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))));
        static::assertNull($constraint->firstViolation(array_to_rows([[
            'id' => 2,
        ]], schema(int_schema('id')))));
        static::assertSame(
            0,
            $constraint->firstViolation(array_to_rows([[
                'id' => 1,
            ]], schema(int_schema('id')))),
        );
    }

    public function test_unique_constraint_violation(): void
    {
        $constraint = new UniqueConstraint('id', 'sub_id');

        static::assertSame('Values: [id<integer> = 1, sub_id<integer> = 1]', $constraint->violation(
            array_to_rows([['id' => 1, 'sub_id' => 1]], schema(int_schema('id'), int_schema('sub_id'))),
            0,
        ));
    }

    public function test_unique_constraint_violation_on_dates(): void
    {
        $constraint = new UniqueConstraint('date');

        static::assertSame('Values: [date<date> = 2025-01-01]', $constraint->violation(array_to_rows([[
            'date' => new DateTimeImmutable('2025-01-01'),
        ]], schema(date_schema('date'))), 0));
    }

    public function test_first_violation_is_the_first_repeated_key_across_batches(): void
    {
        $constraint = new UniqueConstraint('id');

        static::assertNull($constraint->firstViolation(array_to_rows([
            ['id' => 1],
            ['id' => 2],
        ], schema(int_schema('id')))));
        static::assertSame(
            1,
            $constraint->firstViolation(array_to_rows([
                ['id' => 3],
                ['id' => 2],
                ['id' => 3],
            ], schema(int_schema('id')))),
        );
    }

    public function test_first_violation_within_one_batch(): void
    {
        static::assertSame(
            2,
            (new UniqueConstraint('id'))->firstViolation(array_to_rows([
                ['id' => 1],
                ['id' => 2],
                ['id' => 1],
            ], schema(int_schema('id')))),
        );
    }

    public function test_an_empty_batch_has_no_violation(): void
    {
        static::assertNull((new UniqueConstraint('id'))->firstViolation(array_to_rows([], schema(int_schema('id')))));
    }
}
