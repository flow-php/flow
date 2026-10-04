<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\between;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class BetweenTest extends FlowTestCase
{
    public function test_a_definite_false_short_circuits_past_a_null(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('value')->between(lit(10), lit(null)),
            ['value' => 5],
            schema(int_schema('value')),
        ));
    }

    public function test_a_null_bound_that_could_change_the_answer_is_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('value')->between(lit(10), lit(null)),
            ['value' => 50],
            schema(int_schema('value')),
        ));
    }

    public function test_between_exclusive(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE),
            [
                'value' => 11,
            ],
            schema(int_schema('value')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE),
            [
                'value' => 49,
            ],
            schema(int_schema('value')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE),
            [
                'value' => 10,
            ],
            schema(int_schema('value')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE),
            [
                'value' => 50,
            ],
            schema(int_schema('value')),
        ));
    }

    public function test_between_inclusive(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE),
            [
                'value' => 10,
            ],
            schema(int_schema('value')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE),
            [
                'value' => 50,
            ],
            schema(int_schema('value')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE),
            [
                'value' => 9,
            ],
            schema(int_schema('value')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE),
            [
                'value' => 51,
            ],
            schema(int_schema('value')),
        ));
    }

    public function test_between_left_inclusive(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50)),
            [
                'value' => 10,
            ],
            schema(int_schema('value')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50)),
            [
                'value' => 9,
            ],
            schema(int_schema('value')),
        ));
    }

    public function test_between_right_inclusive(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE),
            [
                'value' => 50,
            ],
            schema(int_schema('value')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE),
            [
                'value' => 51,
            ],
            schema(int_schema('value')),
        ));
    }

    public function test_between_with_invalid_boundary_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "object<Flow\ETL\Function\Between\Boundary>", got "string".');

        $context = flow_context();
        (new FunctionContext($context))->eval(
            between(ref('value'), lit(10), lit(50), lit('invalid')),
            [
                'value' => 20,
            ],
            schema(int_schema('value')),
        );
    }
}
