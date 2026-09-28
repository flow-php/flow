<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use Flow\ETL\Adapter\Excel\ExcelDecoder;
use Flow\ETL\Tests\FlowTestCase;

final class ExcelDecoderTest extends FlowTestCase
{
    public function test_decode_captures_headers_from_the_first_row_and_emits_no_row_for_it(): void
    {
        $decoder = new ExcelDecoder();

        static::assertSame([], $decoder->decode([['id', 'name']]));
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $decoder->decode([[1, 'Norbert']])[0]);
    }

    public function test_decode_generates_auto_headers_when_header_is_disabled(): void
    {
        static::assertSame(
            ['e00' => 1, 'e01' => 'Norbert'],
            (new ExcelDecoder(withHeader: false))->decode([[1, 'Norbert']])[0],
        );
    }

    public function test_headers_are_generated_when_header_is_disabled(): void
    {
        $decoder = new ExcelDecoder(withHeader: false);

        static::assertCount(1, $decoder->decode([[1, 2, 3]]));
        static::assertSame(['e00', 'e01', 'e02'], $decoder->headers());
    }

    public function test_headers_are_null_before_the_first_decode(): void
    {
        static::assertNull((new ExcelDecoder())->headers());
    }

    public function test_headers_are_stringified_and_blank_for_non_scalar_cells(): void
    {
        $decoder = new ExcelDecoder();
        $decoder->decode([[1, null, 'x']]);

        static::assertSame(['1', '', 'x'], $decoder->headers());
    }

    public function test_headers_are_the_header_row_after_decode(): void
    {
        $decoder = new ExcelDecoder();

        static::assertSame([], $decoder->decode([['id', 'name']]));
        static::assertSame(['id', 'name'], $decoder->headers());
    }

    public function test_decode_keeps_empty_cells_when_convert_empty_to_null_is_disabled(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => ''],
            (new ExcelDecoder(convertEmptyToNull: false))->decode([['id', 'name'], [1, '']])[0],
        );
    }

    public function test_decode_maps_cells_to_headers(): void
    {
        static::assertSame(
            ['id' => 1, 'name' => 'Norbert'],
            (new ExcelDecoder())->decode([['id', 'name'], [1, 'Norbert']])[0],
        );
    }

    public function test_decode_turns_empty_cells_into_null_by_default(): void
    {
        static::assertSame(['id' => 1, 'name' => null], (new ExcelDecoder())->decode([['id', 'name'], [1, '']])[0]);
    }
}
