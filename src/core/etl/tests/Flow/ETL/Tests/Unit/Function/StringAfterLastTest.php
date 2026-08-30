<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringAfterLastTest extends FlowTestCase
{
    public function test_string_after_last(): void
    {
        static::assertSame('rld', ref('str')
            ->stringAfterLast(ref('needle'))
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_after_last_including_needle(): void
    {
        static::assertSame('orld', ref('str')
            ->stringAfterLast(ref('needle'), includeNeedle: true)
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_after_last_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringAfterLast function requires non-null value');

        ref('str')->stringAfterLast('x')->eval(row(['str' => null]), flow_context());
    }
}
