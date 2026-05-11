<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InvalidArgumentException;

final readonly class SheetRange
{
    public int $endRow;

    public function __construct(
        public Columns $columnRange,
        public int $startRow,
        int $endRow,
        private int $maxRows,
    ) {
        if ($this->startRow < 1) {
            throw new InvalidArgumentException(\sprintf('Start row "%d" must be greater than 0', $this->startRow));
        }

        if ($endRow < 1) {
            throw new InvalidArgumentException(\sprintf('End row "%d" must be greater than 0', $endRow));
        }

        if ($endRow < $this->startRow) {
            throw new InvalidArgumentException(\sprintf(
                'End row "%d" must be greater or equal to start row "%d"',
                $endRow,
                $this->startRow,
            ));
        }

        if ($this->maxRows < 1) {
            throw new InvalidArgumentException(\sprintf('Max rows "%d" must be greater than 0', $this->maxRows));
        }

        $this->endRow = min($endRow, $this->maxRows);
    }

    public function nextRows(int $count): self
    {
        if ($count < 1) {
            throw new InvalidArgumentException(\sprintf('Count "%d" must be greater than 0', $count));
        }

        return new self(
            $this->columnRange,
            min($this->endRow + 1, $this->maxRows),
            min($this->endRow + $count, $this->maxRows),
            $this->maxRows,
        );
    }

    public function toString(): string
    {
        return \sprintf(
            '%s!%s%d:%s%d',
            $this->columnRange->sheetName,
            $this->columnRange->startColumn,
            $this->startRow,
            $this->columnRange->endColumn,
            $this->endRow,
        );
    }
}
