<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringAfterTest extends FlowTestCase
{
    public function test_string_after(): void
    {
        static::assertSame(' world', ref('str')
            ->stringAfter(ref('needle'))
            ->eval(row(['str' => 'hello world', 'needle' => 'hello']), flow_context()));

        static::assertSame(' world', ref('str')
            ->stringAfter(ref('needle'))
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_after_including_needle(): void
    {
        static::assertSame('o world', ref('str')
            ->stringAfter(ref('needle'), includeNeedle: true)
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_after_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringAfter function requires non-null value');

        ref('str')->stringAfter('x')->eval(row(['str' => null]), flow_context());
    }
}
