<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, lit, ref, row};
use Flow\ETL\Tests\FlowTestCase;

final class LogicalFunctionsTest extends FlowTestCase
{
    public function test_logical_operations() : void
    {
        self::assertFalse(ref('id')->isEven()->andNot(ref('id')->equals(lit(1)))->eval(row(int_entry('id', 1))));
        self::assertTrue(ref('id')->isOdd()->and(ref('id')->equals(lit(1)))->eval(row(int_entry('id', 1))));
        self::assertTrue(ref('id')->isEven()->or(ref('id')->equals(lit(1)))->eval(row(int_entry('id', 1))));
        self::assertFalse(ref('id')->isOdd()->andNot(ref('id')->equals(lit(1)))->eval(row(int_entry('id', 1))));
    }
}
