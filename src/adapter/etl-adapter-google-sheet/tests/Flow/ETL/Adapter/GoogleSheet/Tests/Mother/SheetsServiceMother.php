<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Mother;

use Flow\ETL\Adapter\GoogleSheet\Tests\Double\SpySpreadsheetsValuesResource;
use Flow\ETL\Adapter\GoogleSheet\Tests\Double\StubSpreadsheetsResource;
use Google\Service\Sheets;

final class SheetsServiceMother
{
    public static function withSheet(string $sheetName, int $rowCount, SpySpreadsheetsValuesResource $values): Sheets
    {
        $gridProperties = new Sheets\GridProperties();
        $gridProperties->setRowCount($rowCount);

        $properties = new Sheets\SheetProperties();
        $properties->title = $sheetName;
        $properties->setGridProperties($gridProperties);

        $sheet = new Sheets\Sheet();
        $sheet->setProperties($properties);

        $spreadsheet = new Sheets\Spreadsheet();
        $spreadsheet->setSheets([$sheet]);

        $service = new Sheets();
        $service->spreadsheets = new StubSpreadsheetsResource($spreadsheet);
        $service->spreadsheets_values = $values;

        return $service;
    }

    /**
     * getSheets() answers [], so rowCount() falls through to 0 - the absent-sheet path.
     */
    public static function withoutSheets(SpySpreadsheetsValuesResource $values = new SpySpreadsheetsValuesResource()): Sheets
    {
        $service = new Sheets();
        $service->spreadsheets = new StubSpreadsheetsResource(new Sheets\Spreadsheet());
        $service->spreadsheets_values = $values;

        return $service;
    }
}
