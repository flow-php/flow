<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringBeforeTest extends FlowTestCase
{
    public function test_string_before(): void
    {
        static::assertSame('hello ', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'world')), flow_context()));

        static::assertSame('hell', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'o')), flow_context()));
    }

    public function test_string_before_including_needle(): void
    {
        static::assertSame('hello', ref('str')
            ->stringBefore(ref('needle'), includeNeedle: true)
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'o')), flow_context()));
    }

    public function test_string_before_returns_empty_string(): void
    {
        static::assertSame('', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(row(str_entry('str', ''), str_entry('needle', 'o')), flow_context()));
    }

    public function test_string_before_returns_null(): void
    {
        static::assertNull(
            ref('str')
                ->stringBefore(ref('needle'))
                ->eval(row(str_entry('str', null), str_entry('needle', 'o')), flow_context()),
        );
    }
}
