<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class ReverseTest extends FlowTestCase
{
    public function test_reverse_ascii_string(): void
    {
        static::assertSame('olleh', ref('str')->reverse()->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_reverse_empty_string(): void
    {
        static::assertSame('', ref('str')->reverse()->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_reverse_returns_null_for_null_input(): void
    {
        static::assertNull(ref('str')->reverse()->eval(row(str_entry('str', null)), flow_context()));
    }
}
