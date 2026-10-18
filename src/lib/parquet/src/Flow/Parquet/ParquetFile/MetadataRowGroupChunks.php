<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\OutOfBoundsException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function array_key_exists;
use function array_map;
use function array_values;
use function count;

final readonly class MetadataRowGroupChunks implements RowGroupChunks
{
    public function __construct(
        private Metadata $metadata,
    ) {}

    public function chunk(int $rowGroup, FlatColumn $column): ColumnChunk
    {
        return $this->rowGroup($rowGroup)->getColumnChunk($column);
    }

    public function columnOrder(FlatColumn $column): ColumnOrder
    {
        return $this->metadata->columnOrder($column);
    }

    public function count(): int
    {
        return count($this->metadata->rowGroups()->all());
    }

    public function rowCount(int $rowGroup): int
    {
        return $this->rowGroup($rowGroup)->rowsCount();
    }

    public function rowCounts(): array
    {
        return array_values(array_map(
            static fn(RowGroup $rowGroup): int => $rowGroup->rowsCount(),
            $this->metadata->rowGroups()->all(),
        ));
    }

    private function rowGroup(int $rowGroup): RowGroup
    {
        $rowGroups = $this->metadata->rowGroups()->all();

        if (!array_key_exists($rowGroup, $rowGroups)) {
            throw new OutOfBoundsException('Row group ' . $rowGroup . ' does not exist');
        }

        return $rowGroups[$rowGroup];
    }
}
