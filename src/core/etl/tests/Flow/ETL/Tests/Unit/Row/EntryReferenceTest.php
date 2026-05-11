<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class EntryReferenceTest extends FlowTestCase
{
    public function test_executing_equals_expression(): void
    {
        $ref = ref('a')->equals(ref('b'));

        static::assertTrue($ref->eval(row(int_entry('a', 1), int_entry('b', 1)), flow_context()));
    }

    public function test_executing_expression(): void
    {
        $ref = ref('b')->literal(100);

        static::assertSame(100, $ref->eval(row(int_entry('a', 1)), flow_context()));
    }

    public function test_is_even(): void
    {
        $ref = ref('a')->isEven();

        static::assertFalse($ref->eval(row(int_entry('a', 1)), flow_context()));

        static::assertTrue($ref->eval(row(int_entry('a', 2)), flow_context()));
    }

    public function test_is_odd(): void
    {
        $ref = ref('a')->isOdd();

        static::assertTrue($ref->eval(row(int_entry('a', 1)), flow_context()));

        static::assertFalse($ref->eval(row(int_entry('a', 2)), flow_context()));
    }
}
