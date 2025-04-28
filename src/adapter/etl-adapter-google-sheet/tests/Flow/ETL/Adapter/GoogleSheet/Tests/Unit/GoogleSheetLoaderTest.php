<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit;

use function Flow\ETL\DSL\{config,
    date_entry,
    datetime_entry,
    flow_context,
    int_entry,
    row,
    rows,
    string_entry,
    time_entry};
use Flow\ETL\Adapter\GoogleSheet\{GoogleSheetLoader, ValueInputOption};
use Google\Service\Sheets;
use Google\Service\Sheets\{GridProperties, Sheet, SheetProperties, Spreadsheet, ValueRange};
use Google\Service\Sheets\Resource\{Spreadsheets, SpreadsheetsValues};
use PHPUnit\Framework\TestCase;

final class GoogleSheetLoaderTest extends TestCase
{
    public function test_load_with_entity_normalizer_options() : void
    {
        $sheetsMock = $this->createSheetsStub(
            [
                ['date', 'datetime', 'time'],
                ['03-05-2025', '2025-05-03 15:20', '01:00:00'],
            ],
            "'Sheet'!A1:C2"
        );
        $loader = new GoogleSheetLoader(
            $sheetsMock,
            '1234567890',
            'Sheet'
        );
        $loader->withDateTimeFormat('Y-m-d H:i');
        $loader->withDateFormat('d-m-Y');
        $loader->withInputOption(ValueInputOption::RAW);

        $loader->load(
            rows(
                row(
                    date_entry('date', '2025-05-03 15:20:20'),
                    datetime_entry('datetime', '2025-05-03 15:20:20'),
                    time_entry('time', 'PT1H')
                ),
            ),
            flow_context(config())
        );
    }

    public function test_load_with_headers() : void
    {
        $sheetsMock = $this->createSheetsStub(
            [
                ['id', 'name'],
                [12345, 'Norbert'],
                [54321, 'Joseph'],
            ],
            "'Sheet'!A1:B3"
        );
        $loader = new GoogleSheetLoader(
            $sheetsMock,
            '1234567890',
            'Sheet',
        );
        $loader->withInputOption(ValueInputOption::RAW);

        $loader->load(
            rows(
                row(int_entry('id', 12345), string_entry('name', 'Norbert')),
                row(int_entry('id', 54321), string_entry('name', 'Joseph'))
            ),
            flow_context(config())
        );
    }

    public function test_load_without_headers() : void
    {
        $sheetsMock = $this->createSheetsStub(
            [
                [12345, 'Norbert'],
                [54321, 'Joseph'],
            ],
            "'Sheet'!A1:B2"
        );
        $loader = new GoogleSheetLoader(
            $sheetsMock,
            '1234567890',
            'Sheet'
        );
        $loader->withHeader(false);
        $loader->withInputOption(ValueInputOption::RAW);

        $loader->load(
            rows(
                row(int_entry('id', 12345), string_entry('name', 'Norbert')),
                row(int_entry('id', 54321), string_entry('name', 'Joseph'))
            ),
            flow_context(config())
        );
    }

    private function createSheetsStub(array $values, string $cellsRange) : Sheets
    {
        $gridProperties = new GridProperties();
        $gridProperties->rowCount = 1000;

        $sheetProperties = new SheetProperties();
        $sheetProperties->title = 'Sheet';
        $sheetProperties->sheetId = 666;
        $sheetProperties->setGridProperties($gridProperties);

        $sheet = new Sheet();
        $sheet->setProperties($sheetProperties);

        $spreadsheet = new Spreadsheet();
        $spreadsheet->setSpreadsheetId('1234567890');
        $spreadsheet->setSheets([$sheet]);

        $spreadsheetsMock = $this->createMock(Spreadsheets::class);
        $spreadsheetsMock->expects(self::once())->method('get')->with('1234567890')->willReturn($spreadsheet);

        $sheets = new Sheets();
        $sheets->spreadsheets = $spreadsheetsMock;
        $sheets->spreadsheets_values = $this->createMock(SpreadsheetsValues::class);
        $sheets->spreadsheets_values->expects(self::once())->method('update')->with(
            '1234567890',
            $cellsRange,
            new ValueRange(
                [
                    'values' => $values,
                    'majorDimension' => 'ROWS',
                ],
            ),
            ['valueInputOption' => 'RAW']
        )->willReturn(
            [
                'spreadsheetId' => '1234567890',
                'updatedRange' => "'Sheet'!A1:B2",
                'updatedRows' => 2,
                'updatedColumns' => 2,
                'updatedCells' => 4,
            ]
        );

        return $sheets;
    }
}
