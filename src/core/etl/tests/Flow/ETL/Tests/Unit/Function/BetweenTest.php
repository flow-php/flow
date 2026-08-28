<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\between;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class BetweenTest extends FlowTestCase
{
    public function test_between_exclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(
            row(int_entry('value', 11)),
            flow_context(),
        ));
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(
            row(int_entry('value', 49)),
            flow_context(),
        ));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(
            row(int_entry('value', 10)),
            flow_context(),
        ));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(
            row(int_entry('value', 50)),
            flow_context(),
        ));
    }

    public function test_between_inclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(
            row(int_entry('value', 10)),
            flow_context(),
        ));
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(
            row(int_entry('value', 50)),
            flow_context(),
        ));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(
            row(int_entry('value', 9)),
            flow_context(),
        ));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(
            row(int_entry('value', 51)),
            flow_context(),
        ));
    }

    public function test_between_left_inclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50))->eval(row(int_entry('value', 10)), flow_context()));
        static::assertFalse(between(ref('value'), lit(10), lit(50))->eval(row(int_entry('value', 9)), flow_context()));
    }

    public function test_between_right_inclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(
            row(int_entry('value', 50)),
            flow_context(),
        ));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(
            row(int_entry('value', 51)),
            flow_context(),
        ));
    }

    public function test_between_with_invalid_boundary_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Between function requires valid boundary');

        $context = flow_context();
        between(ref('value'), lit(10), lit(50), lit('invalid'))->eval(row(int_entry('value', 20)), $context);
    }
}
