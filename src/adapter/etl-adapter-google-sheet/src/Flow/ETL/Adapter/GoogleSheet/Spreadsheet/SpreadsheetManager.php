<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Spreadsheet;

use Flow\ETL\Exception\InvalidArgumentException;
use Google\Service\Sheets;
use Google\Service\Sheets\{BatchUpdateSpreadsheetRequest, Spreadsheet};

final class SpreadsheetManager
{
    private ?Spreadsheet $spreadsheet = null;

    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
    ) {
    }

    public function getSpreadsheetProperties(string $sheetName) : SpreadsheetProperties
    {
        $rowCount = 0;
        $sheetId = null;

        foreach ($this->spreadsheet()->getSheets() as $sheet) {
            if ($sheetName === $sheet->getProperties()->getTitle()) {
                $sheetId = $sheet->getProperties()->getSheetId();
                $gridProperties = $sheet->getProperties()->getGridProperties();
                $rowCount += $gridProperties->getRowCount();

                break;
            }
        }

        if (null === $sheetId) {
            throw new InvalidArgumentException("Sheet '{$sheetName}' not found in spreadsheet '{$this->spreadsheetId}'");
        }

        return new SpreadsheetProperties($sheetId, $rowCount);
    }

    public function increaseSheetSize(int $currentRow, int $columnsCount, int $sheetId, int $sheetDynamicAllocationSize) : void
    {
        $this->service->spreadsheets->batchUpdate(
            $this->spreadsheet()->spreadsheetId,
            new BatchUpdateSpreadsheetRequest(
                [
                    'requests' => [
                        [
                            'updateSheetProperties' => [
                                'properties' => [
                                    'sheetId' => $sheetId,
                                    'gridProperties' => [
                                        'rowCount' => $currentRow + $sheetDynamicAllocationSize,
                                        // Ensure we update the column size as well
                                        'columnCount' => $columnsCount,
                                    ],
                                ],
                                'fields' => 'gridProperties(rowCount,columnCount)',
                            ],
                        ],
                    ],
                ]
            )
        );
    }

    private function spreadsheet() : Spreadsheet
    {
        if ($this->spreadsheet !== null) {
            return $this->spreadsheet;
        }

        return $this->spreadsheet = $this->service->spreadsheets->get($this->spreadsheetId);
    }
}
