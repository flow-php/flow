<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\Divide;
use Flow\ETL\Functions\Minus;
use Flow\ETL\Functions\Mod;
use Flow\ETL\Functions\Multiply;
use Flow\ETL\Functions\Plus;
use Flow\ETL\Functions\Power;
use Flow\ETL\Functions\Round;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class MathTest extends FlowTestCase
{
    public function test_divide(): void
    {
        static::assertSame(10.0, (new FunctionContext(flow_context()))->eval(
            new Divide(ref('a'), ref('b')),
            ['a' => 100, 'b' => 10],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_minus(): void
    {
        static::assertSame(0, (new FunctionContext(flow_context()))->eval(
            new Minus(ref('a'), ref('b')),
            ['a' => 100, 'b' => 100],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_modulo(): void
    {
        static::assertSame(10, (new FunctionContext(flow_context()))->eval(
            new Mod(ref('a'), ref('b')),
            ['a' => 110, 'b' => 100],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_multiple_operations(): void
    {
        static::assertSame(200, (new FunctionContext(flow_context()))->eval(
            ref('a')->plus(lit(100))->plus(lit(100))->minus(ref('b')),
            ['a' => 100, 'b' => 100],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_multiply(): void
    {
        static::assertSame(10_000, (new FunctionContext(flow_context()))->eval(
            new Multiply(ref('a'), ref('b')),
            ['a' => 100, 'b' => 100],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_plus(): void
    {
        static::assertSame(200, (new FunctionContext(flow_context()))->eval(
            new Plus(ref('a'), ref('b')),
            ['a' => 100, 'b' => 100],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_power(): void
    {
        static::assertSame(1, (new FunctionContext(flow_context()))->eval(
            new Power(ref('a'), ref('b')),
            ['a' => 1, 'b' => 2],
            schema(int_schema('a'), int_schema('b')),
        ));
    }

    public function test_round(): void
    {
        static::assertSame(1.01, (new FunctionContext(flow_context()))->eval(
            new Round(ref('a'), ref('b')),
            ['a' => 1.009, 'b' => 2],
            schema(float_schema('a'), int_schema('b')),
        ));
    }
}
