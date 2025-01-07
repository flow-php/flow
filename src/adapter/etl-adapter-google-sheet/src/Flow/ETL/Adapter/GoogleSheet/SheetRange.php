<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class SheetRange
{
    public function __construct(
        public Columns $columnRange,
        public int $startRow,
        public int $endRow,
    ) {
        if ($this->startRow < 1) {
            throw new InvalidArgumentException(\sprintf('Start row "%d" must be greater than 0', $this->startRow));
        }

        if ($this->endRow < 1) {
            throw new InvalidArgumentException(\sprintf('End row "%d" must be greater than 0', $this->endRow));
        }

        if ($this->endRow < $this->startRow) {
            throw new InvalidArgumentException(\sprintf('End row "%d" must be greater or equal to start row "%d"', $this->endRow, $this->startRow));
        }
    }

    public function nextRows(int $count) : self
    {
        if ($count < 1) {
            throw new InvalidArgumentException(\sprintf('Count "%d" must be greater than 0', $count));
        }

        return new self(
            $this->columnRange,
            $this->endRow + 1,
            $this->endRow + $count,
        );
    }

    public function toString() : string
    {
        return \sprintf(
            '%s!%s%d:%s%d',
            $this->columnRange->sheetName,
            $this->columnRange->startColumn,
            $this->startRow,
            $this->columnRange->endColumn,
            $this->endRow
        );
    }
}
