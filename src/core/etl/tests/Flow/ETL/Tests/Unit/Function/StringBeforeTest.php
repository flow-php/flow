<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringBeforeTest extends FlowTestCase
{
    public function test_string_before(): void
    {
        static::assertSame('hello ', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(row(['str' => 'hello world', 'needle' => 'world']), flow_context()));

        static::assertSame('hell', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_before_including_needle(): void
    {
        static::assertSame('hello', ref('str')
            ->stringBefore(ref('needle'), includeNeedle: true)
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_before_returns_empty_string(): void
    {
        static::assertSame('', ref('str')
            ->stringBefore(ref('needle'))
            ->eval(row(['str' => '', 'needle' => 'o']), flow_context()));
    }

    public function test_string_before_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringBefore function requires non-null value');

        ref('str')->stringBefore(ref('needle'))->eval(row(['str' => null, 'needle' => 'o']), flow_context());
    }
}
