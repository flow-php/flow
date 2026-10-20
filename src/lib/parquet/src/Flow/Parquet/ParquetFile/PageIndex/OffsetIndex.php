<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\PageIndex;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ThriftModel\OffsetIndex as ThriftOffsetIndex;
use Flow\Parquet\ThriftModel\PageLocation as ThriftPageLocation;

use function array_map;
use function array_values;
use function count;
use function intdiv;

final readonly class OffsetIndex
{
    /**
     * @param list<PageLocation> $pages
     */
    public function __construct(
        public array $pages,
    ) {}

    public static function fromThrift(ThriftOffsetIndex $index): self
    {
        return new self(array_values(array_map(
            static fn(ThriftPageLocation $location): PageLocation => PageLocation::fromThrift($location),
            $index->page_locations ?? [],
        )));
    }

    /**
     * Whether the pages lie inside the chunk, each after the previous and a byte at least, and split a row group of
     * $rows rows: the first starts at row 0, each later one after the previous, all before the group's end.
     */
    public function fits(ColumnChunk $chunk, int $rows): bool
    {
        $previousRow = -1;
        $previousEnd = $chunk->pageOffset();
        $chunkEnd = $chunk->pageOffset() + $chunk->totalCompressedSize();

        foreach ($this->pages as $page) {
            $end = $page->offset + $page->compressedPageSize;

            if (
                $page->firstRowIndex <= $previousRow
                || $page->firstRowIndex >= $rows
                || $page->compressedPageSize < 1
                || $page->offset < $previousEnd
                || $end > $chunkEnd
            ) {
                return false;
            }

            $previousRow = $page->firstRowIndex;
            $previousEnd = $end;
        }

        return $this->pages !== [] && $this->pages[0]->firstRowIndex === 0;
    }

    /**
     * The page holding row $row of the row group.
     */
    public function pageOf(int $row): int
    {
        $low = 0;
        $high = count($this->pages) - 1;

        while ($low < $high) {
            $middle = intdiv($low + $high + 1, 2);

            if ($this->pages[$middle]->firstRowIndex <= $row) {
                $low = $middle;
            } else {
                $high = $middle - 1;
            }
        }

        return $low;
    }

    /**
     * Rows of page $page, the last page ending at the row group's $rowGroupRows.
     */
    public function pageRows(int $page, int $rowGroupRows): int
    {
        $next = ($page + 1) < count($this->pages) ? $this->pages[$page + 1]->firstRowIndex : $rowGroupRows;

        return $next - $this->pages[$page]->firstRowIndex;
    }

    public function toThrift(): ThriftOffsetIndex
    {
        return new ThriftOffsetIndex([
            'page_locations' => array_map(
                static fn(PageLocation $page): ThriftPageLocation => $page->toThrift(),
                $this->pages,
            ),
        ]);
    }
}
