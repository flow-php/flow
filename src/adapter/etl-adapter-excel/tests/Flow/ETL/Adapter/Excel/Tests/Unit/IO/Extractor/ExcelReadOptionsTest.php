<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReader;
use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelReadOptions;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class ExcelReadOptionsTest extends FlowTestCase
{
    public function test_defaults_read_the_first_sheet_with_a_header(): void
    {
        $options = new ExcelReadOptions();

        static::assertTrue($options->withHeader);
        static::assertTrue($options->convertEmptyToNull);
        static::assertNull($options->sheetName);
        static::assertNull($options->format);
        static::assertTrue($options->missingColumnsAsNull);
    }

    public function test_an_invalid_sheet_name_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sheet name must be a valid Excel sheet name');

        new ExcelReadOptions(sheetName: 'This is veeeeeery long excel sheet name, longer than 32 characters');
    }

    public function test_an_invalid_sheet_name_is_refused_by_the_wither_too(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sheet name must be a valid Excel sheet name');

        (new ExcelReadOptions())->withSheetName('This is veeeeeery long excel sheet name, longer than 32 characters');
    }

    public function test_each_wither_changes_one_knob_and_keeps_the_rest(): void
    {
        $options = new ExcelReadOptions();

        static::assertEquals(new ExcelReadOptions(withHeader: false), $options->withHeader(false));
        static::assertEquals(new ExcelReadOptions(convertEmptyToNull: false), $options->withConvertEmptyToNull(false));
        static::assertEquals(new ExcelReadOptions(sheetName: 'Sheet2'), $options->withSheetName('Sheet2'));
        static::assertEquals(new ExcelReadOptions(format: ExcelReader::ODS), $options->withFormat(ExcelReader::ODS));
        static::assertEquals(
            new ExcelReadOptions(missingColumnsAsNull: false),
            $options->withMissingColumnsAsNull(false),
        );
    }

    public function test_withers_compose_without_losing_earlier_knobs(): void
    {
        static::assertEquals(
            new ExcelReadOptions(false, false, 'Sheet2', ExcelReader::XLSX),
            (new ExcelReadOptions())
                ->withHeader(false)
                ->withConvertEmptyToNull(false)
                ->withSheetName('Sheet2')
                ->withFormat(ExcelReader::XLSX),
        );
    }

    public function test_normalize_round_trips_with_and_without_a_format(): void
    {
        $xlsx = (new ExcelReadOptions(withHeader: false, sheetName: 'Data'))->withFormat(ExcelReader::XLSX);

        static::assertSame(
            [
                'with_header' => false,
                'convert_empty_to_null' => true,
                'sheet_name' => 'Data',
                'format' => 'xlsx',
                'missing_columns_as_null' => true,
            ],
            $xlsx->normalize(),
        );
        static::assertEquals($xlsx, ExcelReadOptions::fromArray($xlsx->normalize()));
        static::assertNull((new ExcelReadOptions())->normalize()['format']);
        static::assertEquals(
            new ExcelReadOptions(),
            ExcelReadOptions::fromArray((new ExcelReadOptions())->normalize()),
        );
    }
}
