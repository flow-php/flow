<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
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
        $result = ref('str')
            ->wordwrap(10)
            ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context());

        static::assertEquals('', $result);
    }

    public function test_normal_word_wrapping(): void
    {
        $result = ref('str')
            ->wordwrap(10)
            ->eval(array_to_row(['str' => 'The quick brown fox jumps'], schema(str_schema('str'))), flow_context());

        static::assertEquals("The quick\nbrown fox\njumps", $result);
    }

    public function test_null_break_character(): void
    {
        $result = ref('str')
            ->wordwrap(10, ref('break'))
            ->eval(
                array_to_row(
                    ['str' => 'Hello World Test', 'break' => null],
                    schema(str_schema('str'), str_schema('break', nullable: true)),
                ),
                flow_context(),
            );

        static::assertEquals("Hello\nWorld Test", $result);
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Wordwrap function requires non-null value');

        $result = ref('str')
            ->wordwrap(10)
            ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context());

        static::assertNull($result);
    }

    public function test_text_shorter_than_width(): void
    {
        $result = ref('str')
            ->wordwrap(20)
            ->eval(array_to_row(['str' => 'Hello'], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello', $result);
    }

    public function test_width_zero(): void
    {
        $result = ref('str')
            ->wordwrap(0)
            ->eval(array_to_row(['str' => 'Hello World'], schema(str_schema('str'))), flow_context());

        static::assertEquals('Hello World', $result);
    }

    public function test_with_scalar_function_break(): void
    {
        $result = ref('str')
            ->wordwrap(10, ref('break'))
            ->eval(
                array_to_row(
                    ['str' => 'Hello World Test', 'break' => ' | '],
                    schema(str_schema('str'), str_schema('break')),
                ),
                flow_context(),
            );

        static::assertEquals('Hello | World Test', $result);
    }

    public function test_with_scalar_function_cut(): void
    {
        $result = ref('str')
            ->wordwrap(3, "\n", ref('cut'))
            ->eval(
                array_to_row(['str' => 'Hello', 'cut' => true], schema(str_schema('str'), bool_schema('cut'))),
                flow_context(),
            );

        static::assertEquals("Hel\nlo", $result);
    }

    public function test_with_scalar_function_width(): void
    {
        $result = ref('str')
            ->wordwrap(ref('width'))
            ->eval(
                array_to_row(
                    ['str' => 'Hello World Test', 'width' => 8],
                    schema(str_schema('str'), int_schema('width')),
                ),
                flow_context(),
            );

        static::assertEquals("Hello\nWorld\nTest", $result);
    }
}
