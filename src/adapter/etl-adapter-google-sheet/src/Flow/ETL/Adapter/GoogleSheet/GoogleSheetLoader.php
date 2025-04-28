<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\{Adapter\GoogleSheet\RowsNormalizer\EntryNormalizer,
    Adapter\GoogleSheet\Spreadsheet\SpreadsheetManager,
    FlowContext,
    Loader,
    Row\Entry,
    Rows};
use Flow\ETL\Loader\Closure;
use Google\Service\Sheets;
use Google\Service\Sheets\{ValueRange};

final class GoogleSheetLoader implements Closure, Loader
{
    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = 'Y-m-d H:i:s';

    private ValueInputOption $inputOption = ValueInputOption::USER_ENTERED;

    private int $loadedRows = 0;

    private int $sheetDynamicAllocationSize = 1_000;

    private bool $withHeader = true;

    public function __construct(
        private readonly Sheets $service,
        private readonly string $spreadsheetId,
        private readonly string $sheetName,
    ) {
    }

    public function closure(FlowContext $context) : void
    {
        $this->loadedRows = 0;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $manager = new SpreadsheetManager($this->service, $this->spreadsheetId);
        $spreadsheetProperties = $manager->getSpreadsheetProperties($this->sheetName);

        $columnsCount = $rows->first()->entries()->count();
        $currentRow = $this->calculateCurrentRow($rows);
        $sheetRange = $this->calculateSheetAddress($columnsCount, $currentRow);

        // Calculate if spreadsheet size fits current load
        if ($rows->count() >= $spreadsheetProperties->rowCount) {
            $manager->increaseSheetSize($currentRow, $columnsCount, $spreadsheetProperties->sheetId, $this->sheetDynamicAllocationSize);
        }

        $normalizer = new RowsNormalizer(
            new EntryNormalizer($this->dateTimeFormat, $this->dateFormat)
        );

        $this->service->spreadsheets_values->update(
            $this->spreadsheetId,
            $sheetRange->toString(),
            new ValueRange(
                [
                    'values' => $this->values($rows, $normalizer),
                    'majorDimension' => 'ROWS',
                ]
            ),
            $this->inputOption->toArray()
        );

        $this->loadedRows = $currentRow;
    }

    public function withDateFormat(string $dateFormat) : self
    {
        $this->dateFormat = $dateFormat;

        return $this;
    }

    public function withDateTimeFormat(string $dateTimeFormat) : self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withHeader(bool $withHeader) : self
    {
        $this->withHeader = $withHeader;

        return $this;
    }

    public function withInputOption(ValueInputOption $inputOption) : self
    {
        $this->inputOption = $inputOption;

        return $this;
    }

    public function withSheetDynamicAllocationSize(int $bySize) : self
    {
        $this->sheetDynamicAllocationSize = $bySize;

        return $this;
    }

    private function calculateCurrentRow(Rows $rows) : int
    {
        return $rows->count() + (0 === $this->loadedRows ? ($this->withHeader ? 1 : 0) : $this->loadedRows);
    }

    private function calculateSheetAddress(int $columnsCount, int $currentRow) : SheetAddress
    {
        return SheetAddress::calculate(
            $columnsCount,
            $currentRow,
            new SheetAddress(
                startCell: 0 === $this->loadedRows ? 'A1' : 'A' . $this->loadedRows + 1,
                sheetName: $this->sheetName
            )
        );
    }

    private function values(Rows $rows, RowsNormalizer $normalizer) : array
    {
        $values = [];

        if ($this->withHeader && 0 === $this->loadedRows) {
            $values[] = $rows->first()->entries()->map(fn (Entry $entry) => $entry->name());
        }

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            $values[] = $normalizedRow;
        }

        return $values;
    }
}
