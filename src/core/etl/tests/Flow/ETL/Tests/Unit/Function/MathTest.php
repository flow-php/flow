<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Divide;
use Flow\ETL\Function\Minus;
use Flow\ETL\Function\Mod;
use Flow\ETL\Function\Multiply;
use Flow\ETL\Function\Plus;
use Flow\ETL\Function\Power;
use Flow\ETL\Function\Round;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
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
        $row = array_to_row(['a' => 100, 'b' => 10], schema(int_schema('a'), int_schema('b')));

        static::assertSame(10.0, (new Divide(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_minus(): void
    {
        $row = array_to_row(['a' => 100, 'b' => 100], schema(int_schema('a'), int_schema('b')));

        static::assertSame(0, (new Minus(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_modulo(): void
    {
        $row = array_to_row(['a' => 110, 'b' => 100], schema(int_schema('a'), int_schema('b')));

        static::assertSame(10, (new Mod(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_multiple_operations(): void
    {
        static::assertSame(200, ref('a')
            ->plus(lit(100))
            ->plus(lit(100))
            ->minus(ref('b'))
            ->eval(array_to_row(['a' => 100, 'b' => 100], schema(int_schema('a'), int_schema('b'))), flow_context()));
    }

    public function test_multiply(): void
    {
        $row = array_to_row(['a' => 100, 'b' => 100], schema(int_schema('a'), int_schema('b')));

        static::assertSame(10_000, (new Multiply(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_plus(): void
    {
        $row = array_to_row(['a' => 100, 'b' => 100], schema(int_schema('a'), int_schema('b')));

        static::assertSame(200, (new Plus(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_power(): void
    {
        $row = array_to_row(['a' => 1, 'b' => 2], schema(int_schema('a'), int_schema('b')));

        static::assertSame(1, (new Power(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_round(): void
    {
        $row = array_to_row(['a' => 1.009, 'b' => 2], schema(float_schema('a'), int_schema('b')));

        static::assertSame(1.01, (new Round(ref('a'), ref('b')))->eval($row, flow_context()));
    }
}
