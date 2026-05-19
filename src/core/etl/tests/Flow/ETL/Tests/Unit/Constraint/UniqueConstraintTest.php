<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Constraint;

use DateTimeImmutable;
use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;

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

        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 1), int_entry('sub_id', 1))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 1), int_entry('sub_id', 2))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 2), int_entry('sub_id', 1))));
        static::assertFalse($constraint->isSatisfiedBy(row(int_entry('id', 1), int_entry('sub_id', 1))));
    }

    public function test_unique_constraint_on_single_column(): void
    {
        $constraint = new UniqueConstraint('id');

        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 1))));
        static::assertTrue($constraint->isSatisfiedBy(row(int_entry('id', 2))));
        static::assertFalse($constraint->isSatisfiedBy(row(int_entry('id', 1))));
    }

    public function test_unique_constraint_violation(): void
    {
        $constraint = new UniqueConstraint('id', 'sub_id');

        static::assertSame(
            'Values: [id<integer> = 1, sub_id<integer> = 1]',
            $constraint->violation(row(int_entry('id', 1), int_entry('sub_id', 1))),
        );
    }

    public function test_unique_constraint_violation_on_dates(): void
    {
        $constraint = new UniqueConstraint('date');

        static::assertSame(
            'Values: [date<date> = 2025-01-01]',
            $constraint->violation(row(date_entry('date', new DateTimeImmutable('2025-01-01')))),
        );
    }
}
