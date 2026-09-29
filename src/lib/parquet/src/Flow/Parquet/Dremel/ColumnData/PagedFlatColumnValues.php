<?php

declare(strict_types=1);

namespace Flow\Parquet\Dremel\ColumnData;

use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Generator;

final readonly class PagedFlatColumnValues
{
    /**
     * @param Generator<ReadFlatColumnValues> $pages
     */
    public function __construct(
        public FlatColumn $column,
        private Generator $pages,
    ) {}

    /**
     * @return Generator<int, mixed>
     */
    public function assembleFlat(DataConverter $dataConverter): Generator
    {
        foreach ($this->pages as $page) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($page->assembleFlat($dataConverter) as $value) {
                yield $value;
            }
        }
    }

    public function flatPath(): string
    {
        return $this->column->flatPath();
    }

    /**
     * @return Generator<int, FlatValue>
     */
    public function iterator(): Generator
    {
        foreach ($this->pages as $page) {
            foreach ($page->iterator() as $value) {
                yield $value;
            }
        }
    }
}
