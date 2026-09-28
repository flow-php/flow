<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetDecoder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class GoogleSheetDecoderTest extends FlowTestCase
{
    public function test_decodes_rows_with_first_row_as_headers(): void
    {
        $decoder = new GoogleSheetDecoder();

        $decoded = $decoder->decode([
            ['id', 'name'],
            ['1', 'Norbert'],
            ['2', 'Tomek'],
        ]);

        static::assertSame(
            [
                ['id' => '1', 'name' => 'Norbert'],
                ['id' => '2', 'name' => 'Tomek'],
            ],
            $decoded,
        );
    }

    public function test_skips_empty_rows_before_headers(): void
    {
        $decoder = new GoogleSheetDecoder();

        $decoded = $decoder->decode([
            [],
            ['id', 'name'],
            ['1', 'Norbert'],
        ]);

        static::assertSame([['id' => '1', 'name' => 'Norbert']], $decoded);
    }

    public function test_keeps_headers_between_decode_calls(): void
    {
        $decoder = new GoogleSheetDecoder();

        $decoder->decode([['id', 'name'], ['1', 'Norbert']]);

        static::assertSame([['id' => '2', 'name' => 'Tomek']], $decoder->decode([['2', 'Tomek']]));
    }

    public function test_pads_shorter_rows_with_nulls(): void
    {
        $decoder = new GoogleSheetDecoder();

        $decoded = $decoder->decode([
            ['id', 'name'],
            ['1'],
        ]);

        static::assertSame([['id' => '1', 'name' => null]], $decoded);
    }

    public function test_drops_extra_columns(): void
    {
        $decoder = new GoogleSheetDecoder();

        $decoded = $decoder->decode([
            ['id', 'name'],
            ['1', 'Norbert', 'extra'],
        ]);

        static::assertSame([['id' => '1', 'name' => 'Norbert']], $decoded);
    }

    public function test_throws_on_extra_columns_when_drop_is_disabled(): void
    {
        $decoder = new GoogleSheetDecoder(dropExtraColumns: false);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (3) than headers (2)');

        $decoder->decode([
            ['id', 'name'],
            ['1', 'Norbert', 'extra'],
        ]);
    }

    public function test_generates_auto_headers_without_header_row(): void
    {
        $decoder = new GoogleSheetDecoder(withHeader: false);

        $decoded = $decoder->decode([
            ['1', 'Norbert'],
            ['2', 'Tomek'],
        ]);

        static::assertSame(
            [
                ['e00' => '1', 'e01' => 'Norbert'],
                ['e00' => '2', 'e01' => 'Tomek'],
            ],
            $decoded,
        );
    }

    public function test_empty_cells_become_null_by_default(): void
    {
        $decoder = new GoogleSheetDecoder();

        static::assertSame([['a' => null, 'b' => 'x']], $decoder->decode([['a', 'b'], ['', 'x']]));
    }

    public function test_empty_cells_stay_strings_when_asked(): void
    {
        $decoder = new GoogleSheetDecoder(emptyToNull: false);

        static::assertSame([['a' => '', 'b' => 'x']], $decoder->decode([['a', 'b'], ['', 'x']]));
    }

    public function test_omitted_trailing_cells_are_null_regardless_of_empty_to_null(): void
    {
        static::assertSame(
            [['a' => 'x', 'b' => null]],
            (new GoogleSheetDecoder())->decode([
                ['a', 'b'],
                ['x'],
            ]),
        );
        static::assertSame(
            [['a' => 'x', 'b' => null]],
            (new GoogleSheetDecoder(emptyToNull: false))->decode([['a', 'b'], ['x']]),
        );
    }

    public function test_headers_are_empty_before_the_first_row(): void
    {
        static::assertSame([], (new GoogleSheetDecoder())->headers());
    }

    public function test_headers_are_the_consumed_header_row(): void
    {
        $decoder = new GoogleSheetDecoder();
        $decoder->decode([['a', 'b'], ['1', '2']]);

        static::assertSame(['a', 'b'], $decoder->headers());
    }

    public function test_headers_skip_a_leading_empty_row(): void
    {
        $decoder = new GoogleSheetDecoder();
        $decoder->decode([[], ['a', 'b']]);

        static::assertSame(['a', 'b'], $decoder->headers());
    }

    public function test_headers_are_generated_without_a_header_row(): void
    {
        $decoder = new GoogleSheetDecoder(withHeader: false);
        $decoder->decode([['1', '2', '3']]);

        static::assertSame(['e00', 'e01', 'e02'], $decoder->headers());
    }

    public function test_headers_are_strings_even_when_the_api_typed_them(): void
    {
        $decoder = new GoogleSheetDecoder();
        $decoder->decode([[1, '2'], ['x', 'y']]);

        static::assertSame(['1', '2'], $decoder->headers());
    }

    /**
     * b88, pinned as it behaves rather than as it should: `1` and `true` both stringify to `'1'`, and
     * array_combine() keeps the last value under a repeated key, so the row loses a column. Not specific to a
     * typed header - `a,a` does the same in CSV - and not introduced by inference.
     */
    public function test_two_header_cells_with_the_same_name_collapse_the_row_to_one_column(): void
    {
        $decoder = new GoogleSheetDecoder();
        $decoded = $decoder->decode([[1, true], ['x', 'y']]);

        static::assertSame(['1', '1'], $decoder->headers());
        static::assertSame(['1' => 'y'], $decoded[0]);
    }
}
