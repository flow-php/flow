<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Sheet;

use DateInterval;
use DateTimeInterface;
use Generator;
use OpenSpout\Common\Entity\Cell;
use OpenSpout\Reader\SheetInterface;

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
        private int $offset,
    ) {}

    /**
     * @return Generator<int, array<int, SheetCellValue>>
     */
    public function rows(): Generator
    {
        $previousWidth = 0;
        $index = 0;

        foreach ($this->sheet->getRowIterator() as $sheetRow) {
            $index++;

            if ($index === 1 && $this->withHeader) {
                yield array_map(static fn(Cell $cell) => $cell->getValue(), $sheetRow->cells);

                continue;
            }

            if ($this->offset > $index) {
                continue;
            }

            $row = array_map(static fn(Cell $cell) => $cell->getValue(), $sheetRow->cells);

            // the ODS reader drops trailing empty cells; widen to the previous row so columns keep their index
            for ($i = count($row); $i < $previousWidth; $i++) {
                $row[$i] = null;
            }

            $previousWidth = count($row);

            yield $row;
        }
    }
}
