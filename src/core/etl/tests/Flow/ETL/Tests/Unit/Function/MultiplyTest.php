<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class MultiplyTest extends FlowTestCase
{
    public function test_exact_keeps_decimal_arithmetic(): void
    {
        static::assertSame(0.3, (new FunctionContext(flow_context()))->eval(
            ref('a')->multiply(ref('b'), exact: true),
            ['a' => 0.1, 'b' => 3],
            schema(float_schema('a'), int_schema('b')),
        ));
    }

    public function test_floats_use_ieee_arithmetic(): void
    {
        static::assertSame(0.30000000000000004, (new FunctionContext(flow_context()))->eval(
            ref('a')->multiply(ref('b')),
            ['a' => 0.1, 'b' => 3],
            schema(float_schema('a'), int_schema('b')),
        ));
    }

    public function test_integers_stay_integers(): void
    {
        static::assertSame(15, (new FunctionContext(flow_context()))->eval(
            ref('a')->multiply(ref('b')),
            ['a' => 5, 'b' => 3],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_integer_overflow_names_its_row(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage('Multiply function integer overflow (row 0)');

        (new FunctionContext(flow_context()))->eval(
            ref('a')->multiply(ref('b')),
            ['a' => PHP_INT_MAX, 'b' => 2],
            schema(int_schema('a'), int_schema('b')),
        );
    }
}
