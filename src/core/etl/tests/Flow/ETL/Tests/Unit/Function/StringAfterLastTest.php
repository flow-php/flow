<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringAfterLastTest extends FlowTestCase
{
    public function test_string_after_last(): void
    {
        static::assertSame('rld', ref('str')
            ->stringAfterLast(ref('needle'))
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'o')), flow_context()));
    }

    public function test_string_after_last_including_needle(): void
    {
        static::assertSame('orld', ref('str')
            ->stringAfterLast(ref('needle'), includeNeedle: true)
            ->eval(row(str_entry('str', 'hello world'), str_entry('needle', 'o')), flow_context()));
    }

    public function test_string_after_last_returns_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringAfterLast function requires non-null value');

        ref('str')->stringAfterLast('x')->eval(row(str_entry('str', null)), flow_context());
    }
}
