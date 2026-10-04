<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\Grid;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Grid\RecordDecoder;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class RecordDecoderTest extends FlowTestCase
{
    public function test_header_cells_are_trimmed_and_a_blank_one_is_named_by_its_position(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);

        static::assertSame([], $records->decode([[' id ', '', null, 'name ']]));
        static::assertSame(['id', 'e01', 'e02', 'name'], $records->headers());
    }

    public function test_a_numeric_header_cell_is_its_string(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode([[1, 2.5, true]]);

        static::assertSame(['1', '2.5', '1'], $records->headers());
    }

    public function test_a_non_scalar_header_cell_is_named_by_its_position(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode([[new DateTimeImmutable('2026-01-01'), 'name']]);

        static::assertSame(['e00', 'name'], $records->headers());
    }

    public function test_without_a_header_the_first_record_sizes_the_generated_names(): void
    {
        $records = new RecordDecoder(withHeader: false, emptyToNull: true);

        static::assertSame([['e00' => 'a', 'e01' => 'b']], $records->decode([['a', 'b']]));
        static::assertSame(['e00', 'e01'], $records->headers());
    }

    #[TestWith([true])]
    #[TestWith([false])]
    public function test_a_cell_a_short_record_lacks_is_null(bool $emptyToNull): void
    {
        static::assertSame(
            [['id' => '1', 'name' => 'a', 'age' => null]],
            (new RecordDecoder(withHeader: true, emptyToNull: $emptyToNull))->decode([
                ['id', 'name', 'age'],
                ['1', 'a'],
            ]),
        );
    }

    #[TestWith([true, null])]
    #[TestWith([false, ''])]
    public function test_a_present_empty_cell_is_null_only_under_empty_to_null(
        bool $emptyToNull,
        ?string $expected,
    ): void {
        static::assertSame(
            [['id' => '1', 'name' => $expected]],
            (new RecordDecoder(withHeader: true, emptyToNull: $emptyToNull))->decode([['id', 'name'], ['1', '']]),
        );
    }

    public function test_a_non_string_cell_is_kept_as_read(): void
    {
        $at = new DateTimeImmutable('2026-01-01');

        static::assertSame(
            [['at' => $at, 'n' => 0]],
            (new RecordDecoder(withHeader: true, emptyToNull: true))->decode([['at', 'n'], [$at, 0]]),
        );
    }

    public function test_extra_cells_are_dropped_by_default(): void
    {
        static::assertSame(
            [['id' => '1']],
            (new RecordDecoder(withHeader: true, emptyToNull: true))->decode([['id'], ['1', 'extra']]),
        );
    }

    public function test_extra_cells_are_refused_when_they_are_not_dropped(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (2) than headers (1)');

        (new RecordDecoder(withHeader: true, emptyToNull: true, dropExtraColumns: false))->decode([
            ['id'],
            ['1', 'extra'],
        ]);
    }

    public function test_leading_empty_records_are_skipped_before_the_header(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);

        static::assertSame([['id' => '1']], $records->decode([[], [], ['id'], ['1']]));
        static::assertSame(['id'], $records->headers());
    }

    public function test_an_empty_batch_decodes_to_nothing(): void
    {
        static::assertSame([], (new RecordDecoder(withHeader: true, emptyToNull: true))->decode([]));
    }

    public function test_headers_are_null_before_the_first_decode(): void
    {
        static::assertNull((new RecordDecoder(withHeader: true, emptyToNull: true))->headers());
    }

    public function test_the_header_is_kept_between_batches(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode([['id']]);

        static::assertSame([['id' => '2']], $records->decode([['2']]));
    }

    public function test_decode_captures_headers_from_the_first_row_and_emits_no_row_for_it(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

        static::assertSame([], $decoder->decode([['id', 'name']]));
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $decoder->decode([[1, 'Norbert']])[0]);
    }

    public function test_decode_generates_auto_headers_when_header_is_disabled(): void
    {
        static::assertSame(
            ['e00' => 1, 'e01' => 'Norbert'],
            (new RecordDecoder(withHeader: false, emptyToNull: true))->decode([[1, 'Norbert']])[0],
        );
    }

    public function test_headers_are_generated_when_header_is_disabled(): void
    {
        $decoder = new RecordDecoder(withHeader: false, emptyToNull: true);

        static::assertCount(1, $decoder->decode([[1, 2, 3]]));
        static::assertSame(['e00', 'e01', 'e02'], $decoder->headers());
    }

    public function test_excel_headers_are_null_before_the_first_decode(): void
    {
        static::assertNull((new RecordDecoder(withHeader: true, emptyToNull: true))->headers());
    }

    public function test_headers_are_stringified_and_named_by_position_for_blank_cells(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);
        $decoder->decode([[1, null, 'x']]);

        static::assertSame(['1', 'e01', 'x'], $decoder->headers());
    }

    public function test_headers_are_the_header_row_after_decode(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

        static::assertSame([], $decoder->decode([['id', 'name']]));
        static::assertSame(['id', 'name'], $decoder->headers());
    }

    public function test_decode_keeps_empty_cells_when_convert_empty_to_null_is_disabled(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => ''],
            (new RecordDecoder(withHeader: true, emptyToNull: false))->decode([['id', 'name'], [1, '']])[0],
        );
    }

    public function test_decode_maps_cells_to_headers(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Norbert'],
            (new RecordDecoder(withHeader: true, emptyToNull: true))->decode([['id', 'name'], [1, 'Norbert']])[0],
        );
    }

    public function test_decode_turns_empty_cells_into_null_by_default(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => null],
            (new RecordDecoder(withHeader: true, emptyToNull: true))->decode([['id', 'name'], [1, '']])[0],
        );
    }

    public function test_header_names_follow_the_csv_rule(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);
        $decoder->decode([[' id ', '', 'name']]);

        static::assertSame(['id', 'e01', 'name'], $decoder->headers());
    }

    public function test_decodes_rows_with_first_row_as_headers(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

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
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

        $decoded = $decoder->decode([
            [],
            ['id', 'name'],
            ['1', 'Norbert'],
        ]);

        static::assertSame([['id' => '1', 'name' => 'Norbert']], $decoded);
    }

    public function test_keeps_headers_between_decode_calls(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

        $decoder->decode([['id', 'name'], ['1', 'Norbert']]);

        static::assertSame([['id' => '2', 'name' => 'Tomek']], $decoder->decode([['2', 'Tomek']]));
    }

    public function test_pads_shorter_rows_with_nulls(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

        $decoded = $decoder->decode([
            ['id', 'name'],
            ['1'],
        ]);

        static::assertSame([['id' => '1', 'name' => null]], $decoded);
    }

    public function test_drops_extra_columns(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

        $decoded = $decoder->decode([
            ['id', 'name'],
            ['1', 'Norbert', 'extra'],
        ]);

        static::assertSame([['id' => '1', 'name' => 'Norbert']], $decoded);
    }

    public function test_throws_on_extra_columns_when_drop_is_disabled(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true, dropExtraColumns: false);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (3) than headers (2)');

        $decoder->decode([
            ['id', 'name'],
            ['1', 'Norbert', 'extra'],
        ]);
    }

    public function test_generates_auto_headers_without_header_row(): void
    {
        $decoder = new RecordDecoder(withHeader: false, emptyToNull: true);

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
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);

        static::assertSame([['a' => null, 'b' => 'x']], $decoder->decode([['a', 'b'], ['', 'x']]));
    }

    public function test_empty_cells_stay_strings_when_asked(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: false);

        static::assertSame([['a' => '', 'b' => 'x']], $decoder->decode([['a', 'b'], ['', 'x']]));
    }

    public function test_omitted_trailing_cells_are_null_regardless_of_empty_to_null(): void
    {
        static::assertSame(
            [['a' => 'x', 'b' => null]],
            (new RecordDecoder(withHeader: true, emptyToNull: true))->decode([
                ['a', 'b'],
                ['x'],
            ]),
        );
        static::assertSame(
            [['a' => 'x', 'b' => null]],
            (new RecordDecoder(withHeader: true, emptyToNull: false))->decode([['a', 'b'], ['x']]),
        );
    }

    public function test_headers_are_the_consumed_header_row(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);
        $decoder->decode([['a', 'b'], ['1', '2']]);

        static::assertSame(['a', 'b'], $decoder->headers());
    }

    public function test_headers_skip_a_leading_empty_row(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);
        $decoder->decode([[], ['a', 'b']]);

        static::assertSame(['a', 'b'], $decoder->headers());
    }

    public function test_headers_are_generated_without_a_header_row(): void
    {
        $decoder = new RecordDecoder(withHeader: false, emptyToNull: true);
        $decoder->decode([['1', '2', '3']]);

        static::assertSame(['e00', 'e01', 'e02'], $decoder->headers());
    }

    public function test_headers_are_strings_even_when_the_api_typed_them(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);
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
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);
        $decoded = $decoder->decode([[1, true], ['x', 'y']]);

        static::assertSame(['1', '1'], $decoder->headers());
        static::assertSame(['1' => 'y'], $decoded[0]);
    }

    public function test_google_sheet_header_names_follow_the_csv_rule(): void
    {
        $decoder = new RecordDecoder(withHeader: true, emptyToNull: true);
        $decoder->decode([[' id ', '', 'name']]);

        static::assertSame(['id', 'e01', 'name'], $decoder->headers());
    }
}
