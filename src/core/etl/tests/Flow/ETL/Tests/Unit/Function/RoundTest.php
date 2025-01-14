<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{float_entry, lit, row};
use function Flow\ETL\DSL\{ref};
use Flow\ETL\{Tests\FlowTestCase};

final class RoundTest extends FlowTestCase
{
    public function test_round_float() : void
    {
        self::assertEquals(
            10.12,
            ref('float')->round(lit(2))->eval(row(float_entry('float', 10.123)))
        );

        self::assertIsFloat(
            ref('float')->round(lit(2))->eval(row(float_entry('float', 10.123)))
        );
    }

    public function test_round_with_precision_0() : void
    {
        self::assertEquals(
            10,
            ref('float')->round(lit(0))->eval(row(float_entry('float', 10.123)))
        );

        self::assertIsInt(
            ref('float')->round(lit(0))->eval(row(float_entry('float', 10.123)))
        );
    }
}
