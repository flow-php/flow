<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Row\RawRowValues;
use Generator;
use Google\Service\Sheets;

use function array_merge;
use function array_values;
use function count;

final readonly class GoogleSheetReader
{
    public function __construct(
        private Sheets $service,
        private string $spreadsheetId,
        private Columns $columns,
        private GoogleSheetReadOptions $readOptions,
    ) {}

    /**
     * @return Generator<int, list<RawRowValues>>
     */
    public function batches(int $rowsPerPage, int $batchSize): Generator
    {
        $range = new SheetRange($this->columns, 1, $rowsPerPage, $this->rowCount());
        $ranges = [];

        for ($totalRows = 0; $totalRows < $range->endRow; $totalRows += $rowsPerPage) {
            $ranges[] = $range->toString();
            $range = $range->nextRows($rowsPerPage);
        }

        /** @var Sheets\Resource\SpreadsheetsValues $values */
        $values = $this->service->spreadsheets_values;
        $encoder = $this->readOptions->encoder();
        $batch = [];

        foreach ($values->batchGet($this->spreadsheetId, array_merge($this->readOptions->options, [
            'ranges' => $ranges,
        ]))->getValueRanges() as $valueRange) {
            // @mago-ignore analysis:redundant-null-coalesce
            foreach ($encoder->decode(array_values($valueRange->getValues() ?? [])) as $rowValues) {
                $batch[] = $rowValues;

                if (count($batch) >= $batchSize) {
                    yield $batch;
                    $batch = [];
                }
            }
        }

        if ($batch !== []) {
            yield $batch;
        }
    }

    public function rowCount(): int
    {
        /** @var Sheets\Resource\Spreadsheets $spreadsheets */
        $spreadsheets = $this->service->spreadsheets;

        foreach ($spreadsheets->get($this->spreadsheetId, [
            'ranges' => [],
            'includeGridData' => false,
        ])->getSheets() as $sheet) {
            $properties = $sheet->getProperties();

            if ($properties->title === $this->columns->sheetName) {
                return $properties->getGridProperties()->getRowCount();
            }
        }

        return 0;
    }

    /**
     * @param int<1, max>|-1 $rows
     */
    public function sample(int $rows): GoogleSheetSample
    {
        $rowCount = $this->rowCount();
        $range = $rows === -1
            ? new SheetRange($this->columns, 1, $rowCount, $rowCount)
            : new SheetRange($this->columns, 1, $rows + ($this->readOptions->withHeader ? 1 : 0), $rowCount);

        /** @var Sheets\Resource\SpreadsheetsValues $values */
        $values = $this->service->spreadsheets_values;
        $encoder = $this->readOptions->encoder();
        // @mago-ignore analysis:redundant-null-coalesce
        $decoded = $encoder->decode(array_values(
            $values->get($this->spreadsheetId, $range->toString(), $this->readOptions->options)->getValues() ?? [],
        ));

        return new GoogleSheetSample($encoder->headers(), $decoded, $range->endRow >= $rowCount);
    }
}
