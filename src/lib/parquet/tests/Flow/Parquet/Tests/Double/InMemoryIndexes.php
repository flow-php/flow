<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

/**
 * Answers from the indexes it is given, keyed by flat path then row group; records every lookup as [method, path, row group].
 */
final class InMemoryIndexes implements Indexes
{
    /**
     * @var list<array{string, string, int}>
     */
    public array $reads = [];

    /**
     * @param array<string, array<int, ColumnIndex>> $columnIndexes
     * @param array<string, array<int, OffsetIndex>> $offsetIndexes
     * @param array<string, array<int, Sbbf>> $bloomFilters
     */
    public function __construct(
        private readonly array $columnIndexes = [],
        private readonly array $offsetIndexes = [],
        private readonly array $bloomFilters = [],
    ) {}

    public function bloomFilter(int $rowGroup, FlatColumn $column): ?Sbbf
    {
        $this->reads[] = ['bloomFilter', $column->flatPath(), $rowGroup];

        return $this->bloomFilters[$column->flatPath()][$rowGroup] ?? null;
    }

    public function columnIndex(int $rowGroup, FlatColumn $column): ?ColumnIndex
    {
        $this->reads[] = ['columnIndex', $column->flatPath(), $rowGroup];

        return $this->columnIndexes[$column->flatPath()][$rowGroup] ?? null;
    }

    public function offsetIndex(int $rowGroup, FlatColumn $column): ?OffsetIndex
    {
        $this->reads[] = ['offsetIndex', $column->flatPath(), $rowGroup];

        return $this->offsetIndexes[$column->flatPath()][$rowGroup] ?? null;
    }
}
