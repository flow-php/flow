<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Unit\Spreadsheet;

use Flow\ETL\Adapter\GoogleSheet\Spreadsheet\SpreadsheetManager;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Google\Service\Sheets;
use Google\Service\Sheets\Resource\{Spreadsheets};
use Google\Service\Sheets\{Spreadsheet};

final class SpreadsheetManagerTest extends FlowTestCase
{
    public function test_get_spreadsheet_properties_fails_without_sheet() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Sheet 'Sheet' not found in spreadsheet '1234567890'");

        $manager = new SpreadsheetManager($this->createSheetsStub(), '1234567890');
        $manager->getSpreadsheetProperties('Sheet');
    }

    private function createSheetsStub() : Sheets
    {
        $spreadsheet = new Spreadsheet();
        $spreadsheet->setSpreadsheetId('1234567890');
        $spreadsheet->setSheets([]);

        $spreadsheetsMock = $this->createMock(Spreadsheets::class);
        $spreadsheetsMock->expects(self::once())->method('get')->with('1234567890')->willReturn($spreadsheet);

        $sheets = new Sheets();
        $sheets->spreadsheets = $spreadsheetsMock;

        return $sheets;
    }
}
