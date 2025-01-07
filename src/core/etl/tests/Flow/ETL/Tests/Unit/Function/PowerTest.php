<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{int_entry, lit, ref, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class PowerTest extends FlowTestCase
{
    public function test_power_non_numeric_values() : void
    {
        self::assertNull(
            ref('int')->power(lit('non numeric'))->eval(row(int_entry('int', 10)))
        );
        self::assertNull(
            ref('str')->power(lit(2))->eval(row(str_entry('str', 'abc')))
        );
    }

    public function test_power_two_numeric_values() : void
    {
        self::assertSame(
            100,
            ref('int')->power(lit(2))->eval(row(int_entry('int', 10)))
        );
    }
}
