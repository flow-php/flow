<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class IndexOfLastTest extends FlowTestCase
{
    public function test_index_of_last_basic(): void
    {
        static::assertSame(9, ref('str')->indexOfLast('l')->eval(row(str_entry('str', 'hello world')), flow_context()));
    }

    public function test_index_of_last_not_found(): void
    {
        static::assertNull(ref('str')->indexOfLast('x')->eval(row(str_entry('str', 'hello world')), flow_context()));
    }

    public function test_index_of_last_null_needle_returns_false(): void
    {
        static::assertFalse(
            ref('str')
                ->indexOfLast(ref('needle'))
                ->eval(row(str_entry('str', 'hello'), str_entry('needle', null)), flow_context()),
        );
    }

    public function test_index_of_last_null_string_returns_false(): void
    {
        static::assertFalse(ref('str')->indexOfLast('l')->eval(row(str_entry('str', null)), flow_context()));
    }

    public function test_index_of_last_with_scalar_function_parameters(): void
    {
        static::assertSame(9, ref('str')
            ->indexOfLast(ref('needle'), ref('ignore_case'))
            ->eval(
                row(str_entry('str', 'hello world'), str_entry('needle', 'L'), bool_entry('ignore_case', true)),
                flow_context(),
            ));
    }
}
