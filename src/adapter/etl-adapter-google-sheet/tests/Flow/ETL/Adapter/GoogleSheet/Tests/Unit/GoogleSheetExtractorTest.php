<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet_columns;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\{ConfigBuilder, FlowContext, Row, Rows};
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\SpreadsheetsValues;
use PHPUnit\Framework\TestCase;

final class GoogleSheetExtractorTest extends TestCase
{
    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row() : void
    {
        $extractor = from_google_sheet_columns(
            $service = $this->createMock(Sheets::class),
            $spreadSheetId = 'spread-id',
            $sheetName = 'sheet',
            'A',
            'B',
            true,
            2,
        );
        $spreadSheetIdEntry = new StringEntry('_spread_sheet_id', $spreadSheetId);
        $sheetNameEntry = new StringEntry('_sheet_name', $sheetName);
        $firstValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $firstValueRangeMock->method('getValues')->willReturn([
            ['header'],
            ['row1'],
        ]);
        $secondValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $secondValueRangeMock->method('getValues')->willReturn([
            ['row2'],
        ]);
        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));

        $spreadsheetsValues->expects(self::exactly(2))
            ->method('get')
            ->willReturnOnConsecutiveCalls($firstValueRangeMock, $secondValueRangeMock);

        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext((new ConfigBuilder())->putInputIntoRows()->build())));
        self::assertCount(2, $rowsArray);
        self::assertSame(1, $rowsArray[0]->count());
        self::assertEquals(Row::create($sheetNameEntry, $spreadSheetIdEntry, str_entry('header', 'row1')), $rowsArray[0]->first());
        self::assertSame(1, $rowsArray[1]->count());
        self::assertEquals(Row::create($sheetNameEntry, $spreadSheetIdEntry, str_entry('header', 'row2')), $rowsArray[1]->first());
    }

    public function test_rows_in_batch_must_be_positive_integer() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows per page must be greater than 0');

        from_google_sheet_columns(
            $this->createMock(Sheets::class),
            'spread-id',
            'sheet',
            'A',
            'B',
            true,
            0
        );
    }

    public function test_works_for_no_data() : void
    {
        $extractor = from_google_sheet_columns(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            'sheet',
            'A',
            'B',
            true,
            20
        );
        $ValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $ValueRangeMock->method('getValues')->willReturn(null);

        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));
        $spreadsheetsValues->method('get')->willReturn($ValueRangeMock);
        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext((new ConfigBuilder())->build())));
        self::assertCount(0, $rowsArray);
    }
}
