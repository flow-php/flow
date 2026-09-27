<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Unit;

use Flow\ETL\Adapter\Text\TextEncoder;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class TextEncoderTest extends FlowTestCase
{
    public function test_encode_appends_the_new_line_separator_to_the_single_value(): void
    {
        static::assertSame(
            ["first\n", "second\n"],
            (new TextEncoder("\n"))->encode(array_to_rows([
                ['text' => 'first'],
                ['text' => 'second'],
            ], schema(str_schema('text')))),
        );
    }

    public function test_encode_renders_null_as_an_empty_line(): void
    {
        static::assertSame(
            ["\n"],
            (new TextEncoder("\n"))->encode(array_to_rows([[
                'text' => null,
            ]], schema(str_schema('text', nullable: true)))),
        );
    }

    public function test_encode_throws_when_a_row_has_more_than_one_column(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Text data loader supports only a single entry rows, and you have 2 rows.');

        (new TextEncoder())->encode(array_to_rows([['a' => 1, 'b' => 2]], schema(int_schema('a'), int_schema('b'))));
    }

    public function test_encode_of_an_empty_batch_with_more_than_one_column_returns_no_lines(): void
    {
        static::assertSame(
            [],
            (new TextEncoder())->encode(array_to_rows([], schema(int_schema('a'), int_schema('b')))),
        );
    }
}
