<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Webmozart\Assert\Assert;

final class SheetRange
{
    public function __construct(
        public readonly Columns $columnRange,
        public readonly int $startRow,
        public readonly int $endRow,
    ) {
        Assert::greaterThan($startRow, 0);
        Assert::greaterThan($endRow, 0);
        Assert::greaterThanEq($endRow, $startRow);
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
