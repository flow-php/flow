<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\IO\Extractor;

use Flow\ETL\Adapter\GoogleSheet\IO\Inference\GoogleSheetSample;
use Generator;
use Google\Service\Sheets;

use function array_merge;
use function array_values;
use function is_array;

final readonly class GoogleSheetReader
{
    public function __construct(
        private Sheets $service,
        private string $spreadsheetId,
        private Columns $columns,
        private GoogleSheetReadOptions $readOptions,
    ) {}

    /**
     * @return Generator<int, list<mixed>> the raw cells of every row, keyed by its 1-based number in the sheet
     */
    public function rows(int $rowsPerPage): Generator
    {
        $range = new SheetRange($this->columns, 1, $rowsPerPage, $this->rowCount());
        $ranges = [];

        for ($totalRows = 0; $totalRows < $range->endRow; $totalRows += $rowsPerPage) {
            $ranges[] = $range->toString();
            $range = $range->nextRows($rowsPerPage);
        }

        /** @var Sheets\Resource\SpreadsheetsValues $values */
        $values = $this->service->spreadsheets_values;

        foreach (array_values($values->batchGet($this->spreadsheetId, array_merge($this->readOptions->options, [
            'ranges' => $ranges,
        ]))->getValueRanges()) as $page => $valueRange) {
            // @mago-ignore analysis:redundant-null-coalesce
            foreach (array_values($valueRange->getValues() ?? []) as $index => $cells) {
                // page $page covers the rows from 1 + $page * $rowsPerPage on; the API leaves out a page's trailing empty rows
                yield 1 + ($page * $rowsPerPage) + $index => array_values($cells);
            }
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
     * @param null|int<1, max> $rows - null for every row
     */
    public function sample(?int $rows): GoogleSheetSample
    {
        $rowCount = $this->rowCount();
        $range = $rows === null
            ? new SheetRange($this->columns, 1, $rowCount, $rowCount)
            : new SheetRange($this->columns, 1, $rows + ($this->readOptions->withHeader ? 1 : 0), $rowCount);

        /** @var Sheets\Resource\SpreadsheetsValues $values */
        $values = $this->service->spreadsheets_values;
        $decoder = $this->readOptions->decoder();
        $cells = [];
        $decoded = [];

        // @mago-ignore analysis:redundant-null-coalesce
        foreach (array_values(
            $values->get($this->spreadsheetId, $range->toString(), $this->readOptions->options)->getValues() ?? [],
        ) as $index => $row) {
            $cells[$index + 1] = array_values($row);
            $rowValues = $decoder->decode($cells[$index + 1]);

            if (is_array($rowValues)) {
                $decoded[] = $rowValues;
            }
        }

        return new GoogleSheetSample(
            $decoder->headers() ?? [],
            $decoded,
            $cells,
            $range->endRow >= $rowCount,
            $rowCount,
        );
    }
}
