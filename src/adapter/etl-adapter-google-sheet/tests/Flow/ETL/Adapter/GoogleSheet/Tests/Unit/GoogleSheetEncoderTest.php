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
}
