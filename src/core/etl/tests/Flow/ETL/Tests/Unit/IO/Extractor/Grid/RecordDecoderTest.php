<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\Grid;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Grid\RecordDecoder;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class RecordDecoderTest extends FlowTestCase
{
    public function test_header_cells_are_trimmed_and_a_blank_one_is_named_by_its_position(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);

        static::assertNull($records->decode([' id ', '', null, 'name ']));
        static::assertSame(['id', 'e01', 'e02', 'name'], $records->headers());
    }

    public function test_a_numeric_header_cell_is_its_string(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode([1, 2.5, true]);

        static::assertSame(['1', '2.5', '1'], $records->headers());
    }

    public function test_a_non_scalar_header_cell_is_named_by_its_position(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode([new DateTimeImmutable('2026-01-01'), 'name']);

        static::assertSame(['e00', 'name'], $records->headers());
    }

    public function test_without_a_header_the_first_record_sizes_the_generated_names(): void
    {
        $records = new RecordDecoder(withHeader: false, emptyToNull: true);

        static::assertSame(['e00' => 'a', 'e01' => 'b'], $records->decode(['a', 'b']));
        static::assertSame(['e00', 'e01'], $records->headers());
    }

    public function test_preset_headers_decode_the_first_record_as_a_row(): void
    {
        $records = new RecordDecoder(withHeader: false, emptyToNull: true, headers: ['id', 'name']);

        static::assertSame(['id' => '1', 'name' => 'a'], $records->decode(['1', 'a']));
    }

    public function test_preset_headers_refuse_a_header_record(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new RecordDecoder(withHeader: true, emptyToNull: true, headers: ['id']);
    }

    #[TestWith([true, null])]
    #[TestWith([false, ''])]
    public function test_a_present_empty_cell_is_null_only_under_empty_to_null(
        bool $emptyToNull,
        ?string $expected,
    ): void {
        $records = new RecordDecoder(withHeader: true, emptyToNull: $emptyToNull);
        $records->decode(['id', 'name']);

        static::assertSame(['id' => '1', 'name' => $expected], $records->decode(['1', '']));
    }

    public function test_a_non_string_cell_is_kept_as_read(): void
    {
        $at = new DateTimeImmutable('2026-01-01');
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode(['at', 'n']);

        static::assertSame(['at' => $at, 'n' => 0], $records->decode([$at, 0]));
    }

    public function test_a_record_with_a_non_empty_extra_cell_is_corrupted(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode(['id']);

        static::assertSame('2 fields, the header has 1', $records->decode(['1', 'extra']));
    }

    #[TestWith([''])]
    #[TestWith([null])]
    public function test_an_empty_extra_cell_loses_nothing(?string $extra): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode(['id']);

        static::assertSame(['id' => '1'], $records->decode(['1', $extra]));
    }

    public function test_a_record_with_a_missing_cell_is_corrupted(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode(['id', 'name', 'age']);

        static::assertSame('2 fields, the header has 3', $records->decode(['1', 'a']));
    }

    #[TestWith([true])]
    #[TestWith([false])]
    public function test_a_missing_cell_is_null_under_missing_columns_as_null(bool $emptyToNull): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: $emptyToNull, missingColumnsAsNull: true);
        $records->decode(['id', 'name', 'age']);

        static::assertSame(['id' => '1', 'name' => 'a', 'age' => null], $records->decode(['1', 'a']));
    }

    public function test_missing_columns_as_null_still_refuses_a_non_empty_extra_cell(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true, missingColumnsAsNull: true);
        $records->decode(['id']);

        static::assertSame('2 fields, the header has 1', $records->decode(['1', 'extra']));
    }

    #[TestWith([[]])]
    #[TestWith([[null]])]
    public function test_an_empty_record_is_no_row_before_and_after_the_header(array $empty): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);

        static::assertNull($records->decode($empty));
        static::assertNull($records->decode(['id']));
        static::assertNull($records->decode($empty));
        static::assertSame(['id' => '1'], $records->decode(['1']));
    }

    public function test_headers_are_null_before_the_first_decode(): void
    {
        static::assertNull((new RecordDecoder(withHeader: true, emptyToNull: true))->headers());
    }

    /**
     * b88, pinned as it behaves rather than as it should: `1` and `true` both stringify to `'1'`, and the later
     * value wins under a repeated key, so the row loses a column. Not specific to a typed header - `a,a` does the same
     * in CSV - and not introduced by inference.
     */
    public function test_two_header_cells_with_the_same_name_collapse_the_row_to_one_column(): void
    {
        $records = new RecordDecoder(withHeader: true, emptyToNull: true);
        $records->decode([1, true]);

        static::assertSame(['1', '1'], $records->headers());
        static::assertSame(['1' => 'y'], $records->decode(['x', 'y']));
    }
}
