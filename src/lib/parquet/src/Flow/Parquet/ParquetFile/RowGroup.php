<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

final class RowGroup
{
    /**
     * @param array<ColumnChunk> $columnChunks
     * @param int $rowsCount
     */
    public function __construct(
        private readonly array $columnChunks,
        private readonly int $rowsCount,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\RowGroup $thrift) : self
    {
        return new self(
            \array_map(static fn (\Flow\Parquet\Thrift\ColumnChunk $columnChunk) => ColumnChunk::fromThrift($columnChunk), $thrift->columns),
            $thrift->num_rows
        );
    }

    /**
     * @return array<ColumnChunk>
     */
    public function columnChunks() : array
    {
        return $this->columnChunks;
    }

    public function rowsCount() : int
    {
        return $this->rowsCount;
    }
}
