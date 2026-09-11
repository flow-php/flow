<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class RepeatTest extends FlowTestCase
{
    public function test_repeat_empty_string(): void
    {
        static::assertSame('', ref('str')->repeat(3)->eval(row(['str' => '']), flow_context()));
    }

    public function test_repeat_negative_times(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null, positive times');

        ref('str')->repeat(-1)->eval(row(['str' => 'hello']), flow_context());
    }

    public function test_repeat_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null value');

        ref('str')->repeat(3)->eval(row(['str' => null]), flow_context());
    }

    public function test_repeat_string_multiple_times(): void
    {
        static::assertSame('hellohellohello', ref('str')->repeat(3)->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_repeat_with_null_times(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null, positive times');

        ref('str')->repeat(ref('times'))->eval(row(['str' => 'hello', 'times' => null]), flow_context());
    }

    public function test_repeat_with_scalar_function_times(): void
    {
        static::assertSame('hellohello', ref('str')
            ->repeat(ref('times'))
            ->eval(row(['str' => 'hello', 'times' => 2]), flow_context()));
    }

    public function test_repeat_zero_times(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null, positive times');

        ref('str')->repeat(0)->eval(row(['str' => 'hello']), flow_context());
    }
}
