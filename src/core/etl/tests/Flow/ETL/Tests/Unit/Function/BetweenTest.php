<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\between;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class BetweenTest extends FlowTestCase
{
    public function test_a_definite_false_short_circuits_past_a_null(): void
    {
        static::assertFalse(ref('value')->between(lit(10), lit(null))->eval(row(['value' => 5]), flow_context()));
    }

    public function test_a_null_bound_that_could_change_the_answer_is_null(): void
    {
        static::assertNull(ref('value')->between(lit(10), lit(null))->eval(row(['value' => 50]), flow_context()));
    }

    public function test_between_exclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row([
            'value' => 11,
        ]), flow_context()));
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row([
            'value' => 49,
        ]), flow_context()));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row([
            'value' => 10,
        ]), flow_context()));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row([
            'value' => 50,
        ]), flow_context()));
    }

    public function test_between_inclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row([
            'value' => 10,
        ]), flow_context()));
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row([
            'value' => 50,
        ]), flow_context()));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row([
            'value' => 9,
        ]), flow_context()));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row([
            'value' => 51,
        ]), flow_context()));
    }

    public function test_between_left_inclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50))->eval(row(['value' => 10]), flow_context()));
        static::assertFalse(between(ref('value'), lit(10), lit(50))->eval(row(['value' => 9]), flow_context()));
    }

    public function test_between_right_inclusive(): void
    {
        static::assertTrue(between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(row([
            'value' => 50,
        ]), flow_context()));
        static::assertFalse(between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(row([
            'value' => 51,
        ]), flow_context()));
    }

    public function test_between_with_invalid_boundary_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "object<Flow\ETL\Function\Between\Boundary>", got "string".');

        $context = flow_context();
        between(ref('value'), lit(10), lit(50), lit('invalid'))->eval(row(['value' => 20]), $context);
    }
}
