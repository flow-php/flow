<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\IO\Extractor\Sheet;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Generator;
use OpenSpout\Common\Entity\Cell;
use OpenSpout\Common\Entity\Cell\DateTimeCell;
use OpenSpout\Reader\SheetInterface;
use OpenSpout\Reader\XLSX\Sheet as XlsxSheet;

use function array_map;
use function count;

/**
 * @type SheetCellValue = null|array<array-key, mixed>|bool|DateInterval|DateTimeInterface|float|int|string
 */
final readonly class SheetCells
{
    public function __construct(
        private SheetInterface $sheet,
        private bool $withHeader,
    ) {}

    /**
     * Keyed by the row's 1-based number in the sheet.
     *
     * @return Generator<int, array<int, SheetCellValue>>
     */
    public function rows(): Generator
    {
        $previousWidth = 0;
        $index = 0;

        foreach ($this->values() as $number => $row) {
            $index++;

            if ($index === 1 && $this->withHeader) {
                yield $number => $row;

                continue;
            }

            // the ODS reader drops trailing empty cells; widen to the previous row so columns keep their index
            for ($i = count($row); $i < $previousWidth; $i++) {
                $row[$i] = null;
            }

            $previousWidth = count($row);

            yield $number => $row;
        }
    }

    /**
     * @return Generator<int, array<int, SheetCellValue>>
     */
    private function values(): Generator
    {
        if ($this->sheet instanceof XlsxSheet) {
            yield from (new XlsxSheetRows($this->sheet))->values();

            return;
        }

        static $utc = new DateTimeZone('UTC');

        /** @var int $number */
        foreach ($this->sheet->getRowIterator() as $number => $sheetRow) {
            yield $number => array_map(static fn(Cell $cell) => $cell instanceof DateTimeCell
                ? new DateTimeImmutable($cell->getValue()->format('Y-m-d H:i:s.u'), $utc)
                : $cell->getValue(), $sheetRow->cells);
        }
    }
}
