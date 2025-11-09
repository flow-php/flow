<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, int_entry, lit, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class PowerTest extends FlowTestCase
{
    public function test_power_non_numeric_values() : void
    {
        self::assertNull(
            ref('int')->power(lit('non numeric'))->eval(row(int_entry('int', 10)), flow_context())
        );
        self::assertNull(
            ref('str')->power(lit(2))->eval(row(str_entry('str', 'abc')), flow_context())
        );
    }

    public function test_power_two_numeric_values() : void
    {
        self::assertSame(
            100,
            ref('int')->power(lit(2))->eval(row(int_entry('int', 10)), flow_context())
        );
    }
}
