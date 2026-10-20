<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\OutOfBoundsException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

use function array_key_exists;
use function array_map;
use function array_values;

final readonly class MetadataRowGroupChunks implements RowGroupChunks
{
    /**
     * @var list<int>
     */
    private array $rowCounts;

    public function __construct(
        private Metadata $metadata,
    ) {
        $this->rowCounts = array_values(array_map(
            static fn(RowGroup $rowGroup): int => $rowGroup->rowsCount(),
            $this->metadata->rowGroups()->all(),
        ));
    }

    public function chunk(int $rowGroup, FlatColumn $column): ColumnChunk
    {
        return $this->rowGroup($rowGroup)->getColumnChunk($column);
    }

    public function columnOrder(FlatColumn $column): ColumnOrder
    {
        return $this->metadata->columnOrder($column);
    }

    public function rowCounts(): array
    {
        return $this->rowCounts;
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
