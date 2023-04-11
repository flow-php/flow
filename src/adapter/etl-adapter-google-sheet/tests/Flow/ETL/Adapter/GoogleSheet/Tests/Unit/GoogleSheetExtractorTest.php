<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Adapter\GoogleSheet\Columns;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\SpreadsheetsValues;
use PHPUnit\Framework\TestCase;

final class GoogleSheetExtractorTest extends TestCase
{
    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row() : void
    {
        $extractor = new GoogleSheetExtractor(
            $service = $this->createMock(Sheets::class),
            $spreadSheetId ='spread-id',
            new Columns($sheetName = 'sheet', 'A', 'B'),
            true,
            2,
            'row'
        );
        $spreadSheetIdEntry = new StringEntry('spread_sheet_id', $spreadSheetId);
        $sheetNameEntry = new StringEntry('sheet_name', $sheetName);
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

        $spreadsheetsValues->expects($this->exactly(2))
            ->method('get')
            ->willReturnOnConsecutiveCalls($firstValueRangeMock, $secondValueRangeMock);

        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext((new ConfigBuilder())->putInputIntoRows()->build())));
        $this->assertCount(2, $rowsArray);
        $this->assertSame(1, $rowsArray[0]->count());
        $this->assertEquals(Row::create($sheetNameEntry, $spreadSheetIdEntry, new ArrayEntry('row', ['header'=>'row1'])), $rowsArray[0]->first());
        $this->assertSame(1, $rowsArray[1]->count());
        $this->assertEquals(Row::create($sheetNameEntry, $spreadSheetIdEntry, new ArrayEntry('row', ['header'=>'row2'])), $rowsArray[1]->first());
    }

    public function test_works_for_no_data() : void
    {
        $extractor = new GoogleSheetExtractor(
            $service = $this->createMock(Sheets::class),
            'spread-id',
            new Columns('sheet', 'A', 'B'),
            true,
            20
        );
        $ValueRangeMock = $this->createMock(Sheets\ValueRange::class);
        $ValueRangeMock->method('getValues')->willReturn([]);

        $service->spreadsheets_values = ($spreadsheetsValues = $this->createMock(SpreadsheetsValues::class));
        $spreadsheetsValues->method('get')->willReturn($ValueRangeMock);
        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(new FlowContext((new ConfigBuilder())->build())));
        $this->assertCount(0, $rowsArray);
    }
}
