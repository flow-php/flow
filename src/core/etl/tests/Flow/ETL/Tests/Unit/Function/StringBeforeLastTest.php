<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringBeforeLastTest extends FlowTestCase
{
    public function test_string_before_last(): void
    {
        static::assertSame('hello w', ref('str')
            ->stringBeforeLast(ref('needle'))
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_before_last_including_needle(): void
    {
        static::assertSame('hello wo', ref('str')
            ->stringBeforeLast(ref('needle'), includeNeedle: true)
            ->eval(row(['str' => 'hello world', 'needle' => 'o']), flow_context()));
    }

    public function test_string_before_last_returns_empty_string(): void
    {
        static::assertSame('', ref('str')
            ->stringBeforeLast(ref('needle'))
            ->eval(row(['str' => '', 'needle' => 'o']), flow_context()));
    }

    public function test_string_before_last_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringBeforeLast function requires non-null value');

        ref('str')->stringBeforeLast(ref('needle'))->eval(row(['str' => null, 'needle' => 'o']), flow_context());
    }
}
