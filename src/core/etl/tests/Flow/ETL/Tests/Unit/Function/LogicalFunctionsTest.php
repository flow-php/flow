<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class LogicalFunctionsTest extends FlowTestCase
{
    public function test_logical_operations(): void
    {
        static::assertFalse(
            ref('id')
                ->isEven()
                ->andNot(ref('id')->equals(lit(1)))
                ->eval(row(int_entry('id', 1)), flow_context()),
        );
        static::assertTrue(
            ref('id')
                ->isOdd()
                ->and(ref('id')->equals(lit(1)))
                ->eval(row(int_entry('id', 1)), flow_context()),
        );
        static::assertTrue(
            ref('id')
                ->isEven()
                ->or(ref('id')->equals(lit(1)))
                ->eval(row(int_entry('id', 1)), flow_context()),
        );
        static::assertFalse(
            ref('id')
                ->isOdd()
                ->andNot(ref('id')->equals(lit(1)))
                ->eval(row(int_entry('id', 1)), flow_context()),
        );
    }
}
