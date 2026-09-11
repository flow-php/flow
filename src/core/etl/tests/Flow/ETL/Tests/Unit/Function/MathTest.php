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

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class MathTest extends FlowTestCase
{
    public function test_divide(): void
    {
        $row = row(['a' => 100, 'b' => 10]);

        static::assertSame(10.0, (new Divide(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_minus(): void
    {
        $row = row(['a' => 100, 'b' => 100]);

        static::assertSame(0, (new Minus(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_modulo(): void
    {
        $row = row(['a' => 110, 'b' => 100]);

        static::assertSame(10, (new Mod(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_multiple_operations(): void
    {
        static::assertSame(200, ref('a')
            ->plus(lit(100))
            ->plus(lit(100))
            ->minus(ref('b'))
            ->eval(row(['a' => 100, 'b' => 100]), flow_context()));
    }

    public function test_multiply(): void
    {
        $row = row(['a' => 100, 'b' => 100]);

        static::assertSame(10_000, (new Multiply(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_plus(): void
    {
        $row = row(['a' => 100, 'b' => 100]);

        static::assertSame(200, (new Plus(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_power(): void
    {
        $row = row(['a' => 1, 'b' => 2]);

        static::assertSame(1, (new Power(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_round(): void
    {
        $row = row(['a' => 1.009, 'b' => 2]);

        static::assertSame(1.01, (new Round(ref('a'), ref('b')))->eval($row, flow_context()));
    }
}
