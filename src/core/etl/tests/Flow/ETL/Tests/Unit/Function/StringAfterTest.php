<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringAfterTest extends FlowTestCase
{
    public function test_string_after(): void
    {
        static::assertSame(' world', ref('str')
            ->stringAfter(ref('needle'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'hello')), flow_context()));

        static::assertSame(' world', ref('str')
            ->stringAfter(ref('needle'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'o')), flow_context()));
    }

    public function test_string_after_including_needle(): void
    {
        static::assertSame('o world', ref('str')
            ->stringAfter(ref('needle'), includeNeedle: true)
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'o')), flow_context()));
    }

    public function test_string_after_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringAfter function requires non-null value');

        ref('str')->stringAfter('x')->eval(row(str_entry('str', null)), flow_context());
    }
}
