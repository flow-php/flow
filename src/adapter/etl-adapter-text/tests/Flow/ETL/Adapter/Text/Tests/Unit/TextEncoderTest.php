<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Unit;

use Flow\ETL\Adapter\Text\TextEncoder;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class TextEncoderTest extends FlowTestCase
{
    public function test_decode_trims_trailing_new_lines_into_a_single_text_column(): void
    {
        static::assertSame(
            [['text' => 'first'], ['text' => 'second']],
            array_map(
                static fn(RawRowValues $rowValues): array => $rowValues->values,
                (new TextEncoder())->decode(["first\n", "second\r\n"]),
            ),
        );
    }

    public function test_encode_appends_the_new_line_separator_to_the_single_value(): void
    {
        static::assertSame(
            ["first\n", "second\n"],
            (new TextEncoder("\n"))->encode([
                new TypedRowValues(['text' => 'first'], ['text' => type_string()]),
                new TypedRowValues(['text' => 'second'], ['text' => type_string()]),
            ]),
        );
    }

    public function test_encode_renders_null_as_an_empty_line(): void
    {
        static::assertSame(
            ["\n"],
            (new TextEncoder("\n"))->encode([new TypedRowValues(['text' => null], ['text' => type_string()])]),
        );
    }

    public function test_encode_throws_when_a_row_has_more_than_one_column(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Text data loader supports only a single entry rows, and you have 2 rows.');

        (new TextEncoder())->encode([
            new TypedRowValues(['a' => 1, 'b' => 2], ['a' => type_integer(), 'b' => type_integer()]),
        ]);
    }
}
