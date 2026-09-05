<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use Flow\ETL\Adapter\Excel\ExcelReader;
use Flow\ETL\Adapter\Excel\ExcelReadOptions;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

final class ExcelReadOptionsTest extends FlowTestCase
{
    public function test_defaults_read_the_first_sheet_with_a_header(): void
    {
        $options = new ExcelReadOptions();

        static::assertTrue($options->withHeader);
        static::assertTrue($options->convertEmptyToNull);
        static::assertSame(1, $options->offset);
        static::assertNull($options->sheetName);
        static::assertNull($options->format);
    }

    public function test_an_offset_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater or equal to 1');

        new ExcelReadOptions(offset: 0);
    }

    public function test_an_offset_below_one_is_refused_by_the_wither_too(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater or equal to 1');

        (new ExcelReadOptions())->withOffset(-1);
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
        static::assertEquals(new ExcelReadOptions(offset: 5), $options->withOffset(5));
        static::assertEquals(new ExcelReadOptions(sheetName: 'Sheet2'), $options->withSheetName('Sheet2'));
        static::assertEquals(new ExcelReadOptions(format: ExcelReader::ODS), $options->withFormat(ExcelReader::ODS));
    }

    public function test_withers_compose_without_losing_earlier_knobs(): void
    {
        static::assertEquals(
            new ExcelReadOptions(false, false, 5, 'Sheet2', ExcelReader::XLSX),
            (new ExcelReadOptions())
                ->withHeader(false)
                ->withConvertEmptyToNull(false)
                ->withOffset(5)
                ->withSheetName('Sheet2')
                ->withFormat(ExcelReader::XLSX),
        );
    }
}
