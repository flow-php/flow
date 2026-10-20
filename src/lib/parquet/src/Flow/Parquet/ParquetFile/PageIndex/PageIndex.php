<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\PageIndex;

use function count;

/**
 * A column chunk's column index and offset index that agree with each other and with the row group.
 */
final readonly class PageIndex
{
    private function __construct(
        public ColumnIndex $columnIndex,
        public OffsetIndex $offsetIndex,
        private int $rowGroupRows,
    ) {}

    /**
     * Null without both indexes or when they disagree on the pages.
     */
    public static function of(?ColumnIndex $columnIndex, ?OffsetIndex $offsetIndex, int $rowGroupRows): ?self
    {
        if ($columnIndex === null || $offsetIndex === null || $columnIndex->pages() !== count($offsetIndex->pages)) {
            return null;
        }

        return new self($columnIndex, $offsetIndex, $rowGroupRows);
    }

    /**
     * The page holding row $row of the row group.
     */
    public function pageOf(int $row): int
    {
        return $this->offsetIndex->pageOf($row);
    }

    public function pageRows(int $page): int
    {
        return $this->offsetIndex->pageRows($page, $this->rowGroupRows);
    }
}
