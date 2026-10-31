<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVDecoder;
use Flow\ETL\Tests\FlowTestCase;

final class CSVDecoderTest extends FlowTestCase
{
    public function test_the_header_line_is_no_row(): void
    {
        $decoder = new CSVDecoder();

        static::assertNull($decoder->decode($decoder->cells('id,name')));
        static::assertSame(['id', 'name'], $decoder->headers());
    }

    public function test_fields_are_mapped_to_headers(): void
    {
        $decoder = new CSVDecoder();
        $decoder->decode($decoder->cells('id,name'));

        static::assertSame(['id' => '1', 'name' => 'Norbert'], $decoder->decode($decoder->cells('1,Norbert')));
    }

    public function test_auto_headers_are_generated_when_header_is_disabled(): void
    {
        $decoder = new CSVDecoder(withHeader: false);

        static::assertSame(['e00' => '1', 'e01' => 'Norbert'], $decoder->decode($decoder->cells('1,Norbert')));
        static::assertSame(['e00', 'e01'], $decoder->headers());
    }

    public function test_an_empty_field_is_null_by_default_and_kept_when_empty_to_null_is_disabled(): void
    {
        $nulls = new CSVDecoder();
        $nulls->decode($nulls->cells('id,name'));
        $strings = new CSVDecoder(emptyToNull: false);
        $strings->decode($strings->cells('id,name'));

        static::assertSame(['id' => '1', 'name' => null], $nulls->decode($nulls->cells('1,')));
        static::assertSame(['id' => '1', 'name' => ''], $strings->decode($strings->cells('1,')));
    }

    public function test_a_short_line_is_corrupted(): void
    {
        $decoder = new CSVDecoder();
        $decoder->decode($decoder->cells('id,name'));

        static::assertSame('1 fields, the header has 2', $decoder->decode($decoder->cells('1')));
    }

    public function test_a_short_line_is_padded_under_missing_columns_as_null(): void
    {
        $decoder = new CSVDecoder(missingColumnsAsNull: true);
        $decoder->decode($decoder->cells('id,name'));

        static::assertSame(['id' => '1', 'name' => null], $decoder->decode($decoder->cells('1')));
    }

    public function test_a_line_with_a_non_empty_extra_field_is_corrupted(): void
    {
        $decoder = new CSVDecoder();
        $decoder->decode($decoder->cells('id,name'));

        static::assertSame('3 fields, the header has 2', $decoder->decode($decoder->cells('1,Norbert,extra')));
    }

    public function test_a_trailing_comma_loses_nothing(): void
    {
        $decoder = new CSVDecoder();
        $decoder->decode($decoder->cells('id,name'));

        static::assertSame(['id' => '1', 'name' => 'Norbert'], $decoder->decode($decoder->cells('1,Norbert,')));
    }

    public function test_a_blank_line_is_no_row(): void
    {
        $decoder = new CSVDecoder(withHeader: false);

        static::assertNull($decoder->decode($decoder->cells('')));
    }

    public function test_a_custom_separator_enclosure_and_escape_split_the_fields(): void
    {
        $decoder = new CSVDecoder(separator: ';', enclosure: "'", escape: '|');

        static::assertSame(['id', 'a;b'], $decoder->cells("id;'a;b'"));
    }

    public function test_empty_quoted_fields_are_empty_strings(): void
    {
        $decoder = new CSVDecoder(emptyToNull: false);
        $decoder->decode($decoder->cells('id,name'));

        static::assertSame(['id' => '', 'name' => ''], $decoder->decode($decoder->cells('"",""')));
    }

    public function test_headers_are_null_before_any_line_is_decoded(): void
    {
        static::assertNull((new CSVDecoder())->headers());
    }

    public function test_an_empty_header_cell_is_named_by_position(): void
    {
        $decoder = new CSVDecoder();
        $decoder->decode($decoder->cells(',name'));

        static::assertSame(['e00', 'name'], $decoder->headers());
    }
}
