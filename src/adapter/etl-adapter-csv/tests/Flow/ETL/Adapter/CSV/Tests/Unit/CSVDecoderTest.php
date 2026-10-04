<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\CSVDecoder;
use Flow\ETL\Tests\FlowTestCase;

final class CSVDecoderTest extends FlowTestCase
{
    public function test_decode_captures_headers_from_the_first_row_and_emits_no_row_for_it(): void
    {
        $decoder = new CSVDecoder();

        static::assertSame([], $decoder->decode(['id,name']));
        static::assertSame(['id' => '1', 'name' => 'Norbert'], $decoder->decode(['1,Norbert'])[0]);
    }

    public function test_decode_generates_auto_headers_when_header_is_disabled(): void
    {
        static::assertSame(
            ['e00' => '1', 'e01' => 'Norbert'],
            (new CSVDecoder(withHeader: false))->decode(['1,Norbert'])[0],
        );
    }

    public function test_decode_keeps_empty_fields_when_empty_to_null_is_disabled(): void
    {
        static::assertSame(
            ['id' => '1', 'name' => ''],
            (new CSVDecoder(emptyToNull: false))->decode(['id,name', '1,'])[0],
        );
    }

    public function test_decode_maps_fields_to_headers(): void
    {
        static::assertSame(['id' => '1', 'name' => 'Norbert'], (new CSVDecoder())->decode(['id,name', '1,Norbert'])[0]);
    }

    public function test_decode_pads_short_rows_with_null(): void
    {
        static::assertSame(['id' => '1', 'name' => null], (new CSVDecoder())->decode(['id,name', '1'])[0]);
    }

    public function test_decode_truncates_rows_longer_than_the_headers(): void
    {
        static::assertSame(
            ['id' => '1', 'name' => 'Norbert'],
            (new CSVDecoder())->decode(['id,name', '1,Norbert,extra'])[0],
        );
    }

    public function test_decode_turns_empty_fields_into_null_by_default(): void
    {
        static::assertSame(['id' => '1', 'name' => null], (new CSVDecoder())->decode(['id,name', '1,'])[0]);
    }

    public function test_decoding_a_blank_line_yields_a_single_null_field(): void
    {
        static::assertSame(['e00' => null], (new CSVDecoder(withHeader: false, emptyToNull: false))->decode([''])[0]);
    }

    public function test_decoding_a_line_with_a_custom_separator_enclosure_and_escape_yields_strings(): void
    {
        static::assertSame(
            ['id' => '1', 'name' => 'a;b'],
            (new CSVDecoder(separator: ';', enclosure: "'", escape: '|'))->decode(['id;name', "1;'a;b'"])[0],
        );
    }

    public function test_decoding_a_line_with_empty_quoted_fields_yields_empty_strings(): void
    {
        static::assertSame(
            ['id' => '', 'name' => ''],
            (new CSVDecoder(emptyToNull: false))->decode(['id,name', '"",""'])[0],
        );
    }

    public function test_headers_are_null_before_any_line_is_decoded(): void
    {
        static::assertNull((new CSVDecoder())->headers());
    }

    public function test_the_generated_headers_are_exposed_without_a_header_line(): void
    {
        $decoder = new CSVDecoder(withHeader: false);
        $decoder->decode(['1,a']);

        static::assertSame(['e00', 'e01'], $decoder->headers());
    }

    public function test_the_resolved_headers_are_exposed(): void
    {
        $decoder = new CSVDecoder();

        static::assertSame([], $decoder->decode(['id,name']));
        static::assertSame(['id', 'name'], $decoder->headers());
    }

    public function test_an_empty_header_cell_is_named_by_position(): void
    {
        $decoder = new CSVDecoder();
        $decoder->decode([',name']);

        static::assertSame(['e00', 'name'], $decoder->headers());
    }
}
