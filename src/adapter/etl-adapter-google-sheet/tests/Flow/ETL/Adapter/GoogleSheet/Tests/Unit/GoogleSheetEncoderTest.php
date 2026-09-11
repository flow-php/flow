<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\GoogleSheetEncoder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\TypedRowValues;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;

final class GoogleSheetEncoderTest extends FlowTestCase
{
    public function test_decodes_rows_with_first_row_as_headers(): void
    {
        $encoder = new GoogleSheetEncoder();

        $decoded = $encoder->decode([
            ['id', 'name'],
            ['1', 'Norbert'],
            ['2', 'Tomek'],
        ]);

        static::assertSame(
            [
                ['id' => '1', 'name' => 'Norbert'],
                ['id' => '2', 'name' => 'Tomek'],
            ],
            array_map(static fn($rowValues): array => $rowValues->values, $decoded),
        );
    }

    public function test_skips_empty_rows_before_headers(): void
    {
        $encoder = new GoogleSheetEncoder();

        $decoded = $encoder->decode([
            [],
            ['id', 'name'],
            ['1', 'Norbert'],
        ]);

        static::assertSame(
            [['id' => '1', 'name' => 'Norbert']],
            array_map(static fn($rowValues): array => $rowValues->values, $decoded),
        );
    }

    public function test_keeps_headers_between_decode_calls(): void
    {
        $encoder = new GoogleSheetEncoder();

        $encoder->decode([['id', 'name'], ['1', 'Norbert']]);

        static::assertSame(
            [['id' => '2', 'name' => 'Tomek']],
            array_map(static fn($rowValues): array => $rowValues->values, $encoder->decode([['2', 'Tomek']])),
        );
    }

    public function test_pads_shorter_rows_with_nulls(): void
    {
        $encoder = new GoogleSheetEncoder();

        $decoded = $encoder->decode([
            ['id', 'name'],
            ['1'],
        ]);

        static::assertSame(
            [['id' => '1', 'name' => null]],
            array_map(static fn($rowValues): array => $rowValues->values, $decoded),
        );
    }

    public function test_drops_extra_columns(): void
    {
        $encoder = new GoogleSheetEncoder();

        $decoded = $encoder->decode([
            ['id', 'name'],
            ['1', 'Norbert', 'extra'],
        ]);

        static::assertSame(
            [['id' => '1', 'name' => 'Norbert']],
            array_map(static fn($rowValues): array => $rowValues->values, $decoded),
        );
    }

    public function test_throws_on_extra_columns_when_drop_is_disabled(): void
    {
        $encoder = new GoogleSheetEncoder(dropExtraColumns: false);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (3) than headers (2)');

        $encoder->decode([
            ['id', 'name'],
            ['1', 'Norbert', 'extra'],
        ]);
    }

    public function test_generates_auto_headers_without_header_row(): void
    {
        $encoder = new GoogleSheetEncoder(withHeader: false);

        $decoded = $encoder->decode([
            ['1', 'Norbert'],
            ['2', 'Tomek'],
        ]);

        static::assertSame(
            [
                ['e00' => '1', 'e01' => 'Norbert'],
                ['e00' => '2', 'e01' => 'Tomek'],
            ],
            array_map(static fn($rowValues): array => $rowValues->values, $decoded),
        );
    }

    public function test_encode_is_not_supported(): void
    {
        $encoder = new GoogleSheetEncoder();

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Google Sheet adapter is read-only');

        $encoder->encode([new TypedRowValues([], [])]);
    }

    public function test_empty_cells_become_null_by_default(): void
    {
        $encoder = new GoogleSheetEncoder();

        static::assertSame(
            [['a' => null, 'b' => 'x']],
            array_map(static fn($rowValues): array => $rowValues->values, $encoder->decode([['a', 'b'], ['', 'x']])),
        );
    }

    public function test_empty_cells_stay_strings_when_asked(): void
    {
        $encoder = new GoogleSheetEncoder(emptyToNull: false);

        static::assertSame(
            [['a' => '', 'b' => 'x']],
            array_map(static fn($rowValues): array => $rowValues->values, $encoder->decode([['a', 'b'], ['', 'x']])),
        );
    }

    public function test_omitted_trailing_cells_are_null_regardless_of_empty_to_null(): void
    {
        static::assertSame(
            [['a' => 'x', 'b' => null]],
            array_map(static fn($rowValues): array => $rowValues->values, (new GoogleSheetEncoder())->decode([
                ['a', 'b'],
                ['x'],
            ])),
        );
        static::assertSame(
            [['a' => 'x', 'b' => null]],
            array_map(static fn($rowValues): array => $rowValues->values, (new GoogleSheetEncoder(
                emptyToNull: false,
            ))->decode([['a', 'b'], ['x']])),
        );
    }

    public function test_headers_are_empty_before_the_first_row(): void
    {
        static::assertSame([], (new GoogleSheetEncoder())->headers());
    }

    public function test_headers_are_the_consumed_header_row(): void
    {
        $encoder = new GoogleSheetEncoder();
        $encoder->decode([['a', 'b'], ['1', '2']]);

        static::assertSame(['a', 'b'], $encoder->headers());
    }

    public function test_headers_skip_a_leading_empty_row(): void
    {
        $encoder = new GoogleSheetEncoder();
        $encoder->decode([[], ['a', 'b']]);

        static::assertSame(['a', 'b'], $encoder->headers());
    }

    public function test_headers_are_generated_without_a_header_row(): void
    {
        $encoder = new GoogleSheetEncoder(withHeader: false);
        $encoder->decode([['1', '2', '3']]);

        static::assertSame(['e00', 'e01', 'e02'], $encoder->headers());
    }

    public function test_headers_are_strings_even_when_the_api_typed_them(): void
    {
        $encoder = new GoogleSheetEncoder();
        $encoder->decode([[1, '2'], ['x', 'y']]);

        static::assertSame(['1', '2'], $encoder->headers());
    }

    /**
     * b88, pinned as it behaves rather than as it should: `1` and `true` both stringify to `'1'`, and
     * array_combine() keeps the last value under a repeated key, so the row loses a column. Not specific to a
     * typed header - `a,a` does the same in CSV - and not introduced by inference.
     */
    public function test_two_header_cells_with_the_same_name_collapse_the_row_to_one_column(): void
    {
        $encoder = new GoogleSheetEncoder();
        $decoded = $encoder->decode([[1, true], ['x', 'y']]);

        static::assertSame(['1', '1'], $encoder->headers());
        static::assertSame(['1' => 'y'], $decoded[0]->values);
    }
}
