<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class WordwrapTest extends FlowTestCase
{
    public function test_empty_string(): void
    {
        static::assertEquals('', (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(10),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_normal_word_wrapping(): void
    {
        static::assertEquals("The quick\nbrown fox\njumps", (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(10),
            ['str' => 'The quick brown fox jumps'],
            schema(str_schema('str')),
        ));
    }

    public function test_null_break_character(): void
    {
        static::assertEquals("Hello\nWorld Test", (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(10, ref('break')),
            ['str' => 'Hello World Test', 'break' => null],
            schema(str_schema('str'), str_schema('break', nullable: true)),
        ));
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Wordwrap function requires non-null value');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(10),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_text_shorter_than_width(): void
    {
        static::assertEquals('Hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(20),
            ['str' => 'Hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_width_zero(): void
    {
        static::assertEquals('Hello World', (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(0),
            ['str' => 'Hello World'],
            schema(str_schema('str')),
        ));
    }

    public function test_with_scalar_function_break(): void
    {
        static::assertEquals('Hello | World Test', (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(10, ref('break')),
            ['str' => 'Hello World Test', 'break' => ' | '],
            schema(str_schema('str'), str_schema('break')),
        ));
    }

    public function test_with_scalar_function_cut(): void
    {
        static::assertEquals("Hel\nlo", (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(3, "\n", ref('cut')),
            ['str' => 'Hello', 'cut' => true],
            schema(str_schema('str'), bool_schema('cut')),
        ));
    }

    public function test_with_scalar_function_width(): void
    {
        static::assertEquals("Hello\nWorld\nTest", (new FunctionContext(flow_context()))->eval(
            ref('str')->wordwrap(ref('width')),
            ['str' => 'Hello World Test', 'width' => 8],
            schema(str_schema('str'), int_schema('width')),
        ));
    }
}
