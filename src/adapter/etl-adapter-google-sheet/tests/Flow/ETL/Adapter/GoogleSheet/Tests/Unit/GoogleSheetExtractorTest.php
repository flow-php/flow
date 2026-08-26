<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\SpreadsheetsValues;
use Google\Service\Sheets\ValueRange;

use function Flow\ETL\Adapter\GoogleSheet\from_google_sheet_columns;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_entry;
use function iterator_to_array;

final class GoogleSheetExtractorTest extends FlowTestCase
{
    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $sheetName = 'sheet';

        $gridProperties = new Sheets\GridProperties();
        $gridProperties->setRowCount(100);

        $properties = new Sheets\SheetProperties();
        $properties->title = $sheetName;
        $properties->setGridProperties($gridProperties);

        $sheet = new Sheets\Sheet();
        $sheet->setProperties($properties);

        $spreadsheet = $this->createMock(Sheets\Spreadsheet::class);
        $spreadsheet->expects(self::exactly(2))->method('getSheets')->willReturn([$sheet]);

        $resource = $this->createMock(Sheets\Resource\Spreadsheets::class);
        $resource
            ->expects(self::exactly(2))
            ->method('get')
            ->with('spread-id', ['ranges' => [], 'includeGridData' => false])
            ->willReturn($spreadsheet);

        $service = new Sheets();
        $service->spreadsheets = $resource;

        $valueRange = new ValueRange();
        $valueRange->setValues([['header'], ['row1']]);

        $response = new Sheets\BatchGetValuesResponse();
        $response->setValueRanges([$valueRange]);

        $spreadsheetsValues = $this->createMock(SpreadsheetsValues::class);
        $spreadsheetsValues->expects(self::exactly(2))->method('batchGet')->willReturn($response);

        $service->spreadsheets_values = $spreadsheetsValues;

        $schema = schema(str_schema('header'));

        $extractor = from_google_sheet_columns($service, 'spread-id', $sheetName, 'A', 'B')
            ->withHeader(true)
            ->withRowsPerPage(2)
            ->withSchema($schema)
            ->withMetadataColumns(true);

        iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->build())));
        iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->build())));

        static::assertNull($schema->findDefinition('_spread_sheet_id'));
        static::assertNull($schema->findDefinition('_sheet_name'));
    }

    public function test_its_fails_if_sheet_not_found(): void
    {
        $spreadsheet = $this->createMock(Sheets\Spreadsheet::class);
        $spreadsheet->expects(self::once())->method('getSheets')->willReturn([]);

        $resource = $this->createMock(Sheets\Resource\Spreadsheets::class);
        $resource
            ->expects(self::once())
            ->method('get')
            ->with('spread-id', ['ranges' => [], 'includeGridData' => false])
            ->willReturn($spreadsheet);

        $service = $this->createStub(Sheets::class);
        $service->spreadsheets = $resource;

        $extractor = from_google_sheet_columns($service, 'spread-id', 'sheet', 'A', 'B')
            ->withHeader(true)
            ->withRowsPerPage(2)
            ->withMetadataColumns(true);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Max rows "0" must be greater than 0');

        iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->build())));
    }

    public function test_its_stop_fetching_data_if_processed_row_count_is_less_then_last_range_end_row(): void
    {
        $sheetName = 'sheet';

        $service = $this->createGoogleService($sheetName);

        $extractor = from_google_sheet_columns($service, $spreadSheetId = 'spread-id', $sheetName, 'A', 'B')
            ->withHeader(true)
            ->withRowsPerPage(2)
            ->withMetadataColumns(true);

        $firstValueRangeMock = new ValueRange();
        $firstValueRangeMock->setValues([['header'], ['row1']]);
        $secondValueRangeMock = new ValueRange();
        $secondValueRangeMock->setValues([['row2']]);

        $response = new Sheets\BatchGetValuesResponse();
        $response->setValueRanges([$firstValueRangeMock, $secondValueRangeMock]);

        $spreadsheetsValues = $this->createMock(SpreadsheetsValues::class);
        $spreadsheetsValues->expects(self::once())->method('batchGet')->willReturn($response);

        $service->spreadsheets_values = $spreadsheetsValues;

        $spreadSheetIdEntry = string_entry('_spread_sheet_id', $spreadSheetId);
        $sheetNameEntry = string_entry('_sheet_name', $sheetName);

        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->build())));
        static::assertCount(2, $rowsArray);
        static::assertSame(1, $rowsArray[0]->count());
        static::assertEquals(
            row($sheetNameEntry, $spreadSheetIdEntry, str_entry('header', 'row1')),
            $rowsArray[0]->first(),
        );
        static::assertSame(1, $rowsArray[1]->count());
        static::assertEquals(
            row($sheetNameEntry, $spreadSheetIdEntry, str_entry('header', 'row2')),
            $rowsArray[1]->first(),
        );
    }

    public function test_rows_in_batch_must_be_positive_integer(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Rows per page must be greater than 0');

        from_google_sheet_columns($this->createStub(Sheets::class), 'spread-id', 'sheet', 'A', 'B')->withRowsPerPage(0);
    }

    public function test_schema_appends_the_metadata_columns(): void
    {
        static::assertEquals(
            schema(str_schema('header'), str_schema('_spread_sheet_id'), str_schema('_sheet_name')),
            from_google_sheet_columns(new Sheets(), 'spread-id', 'sheet', 'A', 'B')
                ->withSchema(schema(str_schema('header')))
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_schema_is_refused_when_it_was_not_declared(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot describe what it will produce before producing it');

        from_google_sheet_columns(new Sheets(), 'spread-id', 'sheet', 'A', 'B')->schema();
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('header')),
            from_google_sheet_columns(new Sheets(), 'spread-id', 'sheet', 'A', 'B')
                ->withSchema(schema(str_schema('header')))
                ->schema(),
        );
    }

    public function test_works_for_no_data(): void
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
        $spreadsheetsValues->expects(self::once())->method('batchGet')->willReturn($response);

        $service->spreadsheets_values = $spreadsheetsValues;

        /** @var array<Rows> $rowsArray */
        $rowsArray = iterator_to_array($extractor->extract(flow_context((new ConfigBuilder())->build())));
        static::assertCount(0, $rowsArray);
    }

    private function createGoogleService(string $sheetName): Sheets
    {
        $gridProperties = new Sheets\GridProperties();
        $gridProperties->setRowCount(100);

        $properties = new Sheets\SheetProperties();
        $properties->title = $sheetName;
        $properties->setGridProperties($gridProperties);

        $sheet = new Sheets\Sheet();
        $sheet->setProperties($properties);

        $spreadsheet = $this->createMock(Sheets\Spreadsheet::class);
        $spreadsheet->expects(self::once())->method('getSheets')->willReturn([$sheet]);

        $resource = $this->createMock(Sheets\Resource\Spreadsheets::class);
        $resource
            ->expects(self::once())
            ->method('get')
            ->with('spread-id', ['ranges' => [], 'includeGridData' => false])
            ->willReturn($spreadsheet);

        $service = new Sheets();
        $service->spreadsheets = $resource;

        return $service;
    }
}
