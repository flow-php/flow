<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet;

use Webmozart\Assert\Assert;

final class GoogleSheetRange
{
    private function __construct(
        public readonly string $sheet,
        public readonly string $startColumn,
        public readonly int $startRow,
        public readonly string $endColumn,
        public readonly int $endRow,
    ) {
        Assert::notEmpty($sheet);
        Assert::unicodeLetters($startColumn);
        Assert::unicodeLetters($endColumn);
        Assert::greaterThan($startRow, 0);
        Assert::greaterThan($endRow, 0);
        Assert::greaterThanEq($endRow, $startRow);
        Assert::greaterThanEq($endColumn, $startColumn);
    }

    public static function create(string $sheet, string $startColumn, int $startRow, string $endColumn, int $endRow) : self
    {
        return new self($sheet, $startColumn, $startRow, $endColumn, $endRow);
    }

    public function nextRows(int $count) : self
    {
        Assert::greaterThan($count, 0);

        return new self(
            $this->sheet,
            $this->startColumn,
            $this->endRow + 1,
            $this->endColumn,
            $this->endRow + $count + 1,
        );
    }

    public function toString() : string
    {
        return  \sprintf(
            '%s!%s%d:%s%d',
            $this->sheet,
            $this->startColumn,
            $this->startRow,
            $this->endColumn,
            $this->endRow
        );
    }
}
