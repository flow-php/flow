<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class EqualsTest extends FlowTestCase
{
    public function test_null_operand_yields_null(): void
    {
        static::assertNull(ref('a')->equals(lit(1))->eval(row(int_entry('a', null)), flow_context()));
        static::assertNull(
            ref('a')->equals(ref('b'))->eval(row(int_entry('a', null), int_entry('b', null)), flow_context()),
        );
    }

    public function test_equal_values(): void
    {
        static::assertTrue(ref('a')->equals(lit(1))->eval(row(int_entry('a', 1)), flow_context()));
        static::assertTrue(ref('a')->equals(lit('x'))->eval(row(str_entry('a', 'x')), flow_context()));
        static::assertFalse(ref('a')->equals(lit(2))->eval(row(int_entry('a', 1)), flow_context()));
    }
}
