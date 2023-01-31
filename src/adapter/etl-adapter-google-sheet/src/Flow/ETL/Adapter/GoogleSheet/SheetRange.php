<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Flow\ETL\Exception\InvalidArgumentException;
use Webmozart\Assert\Assert;

final class SheetRange
{
    public function __construct(
        public readonly Columns $columnRange,
        public readonly int $startRow,
        public readonly int $endRow,
    ) {
        if ($this->startRow<=0) {
            throw new InvalidArgumentException(\sprintf(
                'Start row `%d` must be greater than 0',
                $this->startRow
            ));
        }

        if ($this->endRow<=0) {
            throw new InvalidArgumentException(\sprintf(
                'End row `%d` must be greater than 0',
                $this->endRow
            ));
        }

        if ($this->endRow<$this->startRow) {
            throw new InvalidArgumentException(\sprintf(
                'End row `%d` must be greater or equal to start row `%d`',
                $this->endRow,
                $this->startRow
            ));
        }
    }

    public function nextRows(int $count) : self
    {
        Assert::greaterThan($count, 0);

        return new self(
            $this->columnRange,
            $this->endRow + 1,
            $this->endRow + $count,
        );
    }

    public function toString() : string
    {
        return  \sprintf(
            '%s!%s%d:%s%d',
            $this->columnRange->sheetName,
            $this->columnRange->startColumn,
            $this->startRow,
            $this->columnRange->endColumn,
            $this->endRow
        );
    }
}
