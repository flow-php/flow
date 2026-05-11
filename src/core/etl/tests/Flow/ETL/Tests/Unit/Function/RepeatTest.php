<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class RepeatTest extends FlowTestCase
{
    public function test_repeat_empty_string(): void
    {
        static::assertSame('', ref('str')->repeat(3)->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_repeat_negative_times(): void
    {
        static::assertSame('', ref('str')->repeat(-1)->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_repeat_null_input(): void
    {
        static::assertNull(ref('str')->repeat(3)->eval(row(str_entry('str', null)), flow_context()));
    }

    public function test_repeat_string_multiple_times(): void
    {
        static::assertSame('hellohellohello', ref('str')
            ->repeat(3)
            ->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_repeat_with_null_times(): void
    {
        static::assertSame('', ref('str')
            ->repeat(ref('times'))
            ->eval(row(str_entry('str', 'hello'), str_entry('times', null)), flow_context()));
    }

    public function test_repeat_with_scalar_function_times(): void
    {
        static::assertSame('hellohello', ref('str')
            ->repeat(ref('times'))
            ->eval(row(str_entry('str', 'hello'), int_entry('times', 2)), flow_context()));
    }

    public function test_repeat_zero_times(): void
    {
        static::assertSame('', ref('str')->repeat(0)->eval(row(str_entry('str', 'hello')), flow_context()));
    }
}
