<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet_columns;
use function Flow\ETL\DSL\{flow_context, row};
use function Flow\ETL\DSL\{str_entry, string_entry};
use Flow\ETL\{Config\ConfigBuilder, Rows, Tests\FlowTestCase};
use Flow\ETL\Exception\InvalidArgumentException;
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\SpreadsheetsValues;
use Google\Service\Sheets\ValueRange;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    public function test_its_fails_if_sheet_not_found() : void
    {
        $spreadsheet = $this->createMock(Sheets\Spreadsheet::class);
        $spreadsheet->expects(self::once())
            ->method('getSheets')
            ->willReturn([]);

        $resource = $this->createMock(Sheets\Resource\Spreadsheets::class);
        $resource->expects(self::once())
            ->method('get')
            ->with('spread-id', ['ranges' => [], 'includeGridData' => false])
            ->willReturn($spreadsheet);

        $service = $this->createMock(Sheets::class);
        $service->spreadsheets = $resource;

        $extractor = from_google_sheet_columns($service, 'spread-id', 'sheet', 'A', 'B')
            ->withHeader(true)
            ->withRowsPerPage(2);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max rows "0" must be greater than 0');

        \iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->putInputIntoRows()->build())));
    }

    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row() : void
    {
        $sheetName = 'sheet';

        $service = $this->createGoogleService($sheetName);

        $extractor = from_google_sheet_columns($service, $spreadSheetId = 'spread-id', $sheetName, 'A', 'B')
            ->withHeader(true)
            ->withRowsPerPage(2);

        $firstValueRangeMock = new ValueRange();
        $firstValueRangeMock->setValues([['header'], ['row1']]);
        $secondValueRangeMock = new ValueRange();
        $secondValueRangeMock->setValues([['row2']]);

        $response = new Sheets\BatchGetValuesResponse();
        $response->setValueRanges([$firstValueRangeMock, $secondValueRangeMock]);

        $spreadsheetsValues = $this->createMock(SpreadsheetsValues::class);
        $spreadsheetsValues->expects(self::once())
            ->method('batchGet')
            ->willReturn($response);

        $service->spreadsheets_values = $spreadsheetsValues;

        $spreadSheetIdEntry = string_entry('_spread_sheet_id', $spreadSheetId);
        $sheetNameEntry = string_entry('_sheet_name', $sheetName);

        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->putInputIntoRows()->build())));
        self::assertCount(2, $rowsArray);
        self::assertSame(1, $rowsArray[0]->count());
        self::assertEquals(row($sheetNameEntry, $spreadSheetIdEntry, str_entry('header', 'row1')), $rowsArray[0]->first());
        self::assertSame(1, $rowsArray[1]->count());
        self::assertEquals(row($sheetNameEntry, $spreadSheetIdEntry, str_entry('header', 'row2')), $rowsArray[1]->first());
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
        )->withRowsPerPage(0);
    }

    public function test_works_for_no_data() : void
    {
        $service = $this->createGoogleService('sheet');

        $extractor = from_google_sheet_columns($service, 'spread-id', 'sheet', 'A', 'B')
            ->withHeader(true)
            ->withRowsPerPage(20);

        $valueRangeMock = new ValueRange();
        $valueRangeMock->setValues([]);

        $response = new Sheets\BatchGetValuesResponse();
        $response->setValueRanges([$valueRangeMock]);

        $spreadsheetsValues = $this->createMock(SpreadsheetsValues::class);
        $spreadsheetsValues->expects(self::once())
            ->method('batchGet')
            ->willReturn($response);

        $service->spreadsheets_values = $spreadsheetsValues;

        /** @var array<Rows> $rowsArray */
        $rowsArray = \iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->build())));
        self::assertCount(0, $rowsArray);
    }

    private function createGoogleService(string $sheetName) : Sheets
    {
        $gridProperties = new Sheets\GridProperties();
        $gridProperties->setRowCount(100);

        $properties = new Sheets\SheetProperties();
        $properties->title = $sheetName;
        $properties->setGridProperties($gridProperties);

        $sheet = new Sheets\Sheet();
        $sheet->setProperties($properties);

        $spreadsheet = $this->createMock(Sheets\Spreadsheet::class);
        $spreadsheet->expects(self::once())
            ->method('getSheets')
            ->willReturn([$sheet]);

        $resource = $this->createMock(Sheets\Resource\Spreadsheets::class);
        $resource->expects(self::once())
            ->method('get')
            ->with('spread-id', ['ranges' => [], 'includeGridData' => false])
            ->willReturn($spreadsheet);

        $service = new Sheets();
        $service->spreadsheets = $resource;

        return $service;
    }
}
