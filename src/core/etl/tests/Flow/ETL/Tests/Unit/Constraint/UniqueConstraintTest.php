<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Constraint;

use DateTimeImmutable;
use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
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

        static::assertTrue($constraint->isSatisfiedBy(
            array_to_row(['id' => 1, 'sub_id' => 1], schema(int_schema('id'), int_schema('sub_id'))),
            schema(int_schema('id'), int_schema('sub_id')),
        ));
        static::assertTrue($constraint->isSatisfiedBy(
            array_to_row(['id' => 1, 'sub_id' => 2], schema(int_schema('id'), int_schema('sub_id'))),
            schema(int_schema('id'), int_schema('sub_id')),
        ));
        static::assertTrue($constraint->isSatisfiedBy(
            array_to_row(['id' => 2, 'sub_id' => 1], schema(int_schema('id'), int_schema('sub_id'))),
            schema(int_schema('id'), int_schema('sub_id')),
        ));
        static::assertFalse($constraint->isSatisfiedBy(
            array_to_row(['id' => 1, 'sub_id' => 1], schema(int_schema('id'), int_schema('sub_id'))),
            schema(int_schema('id'), int_schema('sub_id')),
        ));
    }

    public function test_unique_constraint_on_single_column(): void
    {
        $constraint = new UniqueConstraint('id');

        static::assertTrue($constraint->isSatisfiedBy(array_to_row([
            'id' => 1,
        ], schema(int_schema('id'))), schema(int_schema('id'))));
        static::assertTrue($constraint->isSatisfiedBy(array_to_row([
            'id' => 2,
        ], schema(int_schema('id'))), schema(int_schema('id'))));
        static::assertFalse($constraint->isSatisfiedBy(array_to_row([
            'id' => 1,
        ], schema(int_schema('id'))), schema(int_schema('id'))));
    }

    public function test_unique_constraint_violation(): void
    {
        $constraint = new UniqueConstraint('id', 'sub_id');

        static::assertSame('Values: [id<integer> = 1, sub_id<integer> = 1]', $constraint->violation(
            array_to_row(['id' => 1, 'sub_id' => 1], schema(int_schema('id'), int_schema('sub_id'))),
            schema(int_schema('id'), int_schema('sub_id')),
        ));
    }

    public function test_unique_constraint_violation_on_dates(): void
    {
        $constraint = new UniqueConstraint('date');

        static::assertSame('Values: [date<date> = 2025-01-01]', $constraint->violation(array_to_row([
            'date' => new DateTimeImmutable('2025-01-01'),
        ], schema(datetime_schema('date'))), schema(date_schema('date'))));
    }
}
