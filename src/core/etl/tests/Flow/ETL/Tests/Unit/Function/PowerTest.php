<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PowerTest extends FlowTestCase
{
    public function test_power_non_numeric_values(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "integer", got "string".');

        (new FunctionContext(flow_context()))->eval(
            ref('int')->power(lit('non numeric')),
            ['int' => 10],
            schema(int_schema('int')),
        );
        (new FunctionContext(flow_context()))->eval(
            ref('str')->power(lit(2)),
            ['str' => 'abc'],
            schema(str_schema('str')),
        );
    }

    public function test_power_two_numeric_values(): void
    {
        static::assertSame(100, (new FunctionContext(flow_context()))->eval(
            ref('int')->power(lit(2)),
            ['int' => 10],
            schema(int_schema('int')),
        ));
    }

    public function test_exact_keeps_decimal_arithmetic(): void
    {
        static::assertSame(1.21, (new FunctionContext(flow_context()))->eval(
            ref('a')->power(ref('b'), exact: true),
            ['a' => 1.1, 'b' => 2],
            schema(float_schema('a'), int_schema('b')),
        ));
    }

    public function test_floats_use_ieee_arithmetic(): void
    {
        static::assertSame(1.2100000000000002, (new FunctionContext(flow_context()))->eval(
            ref('a')->power(ref('b')),
            ['a' => 1.1, 'b' => 2],
            schema(float_schema('a'), int_schema('b')),
        ));
    }

    public function test_a_negative_exponent_of_an_integer_is_refused(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage('Power function of an integer requires a non-negative exponent (row 0)');

        (new FunctionContext(flow_context()))->eval(
            ref('a')->power(ref('b')),
            ['a' => 2, 'b' => -1],
            schema(int_schema('a'), int_schema('b')),
        );
    }

    public function test_integer_overflow_names_its_row(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage('Power function integer overflow (row 0)');

        (new FunctionContext(flow_context()))->eval(
            ref('a')->power(ref('b')),
            ['a' => 10, 'b' => 30],
            schema(int_schema('a'), int_schema('b')),
        );
    }
}
