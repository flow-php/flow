<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class PowerTest extends FlowTestCase
{
    public function test_power_non_numeric_values(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Power function requires non-null values');

        ref('int')->power(lit('non numeric'))->eval(row(int_entry('int', 10)), flow_context());
        ref('str')->power(lit(2))->eval(row(str_entry('str', 'abc')), flow_context());
    }

    public function test_power_two_numeric_values(): void
    {
        static::assertSame(100, ref('int')->power(lit(2))->eval(row(int_entry('int', 10)), flow_context()));
    }
}
