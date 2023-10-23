<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

final class RowGroup
{
    /**
     * @param array<ColumnChunk> $columnChunks
     * @param int $rowsCount
     */
    public function __construct(
        private array $columnChunks,
        private int $rowsCount,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\RowGroup $thrift) : self
    {
        return new self(
            \array_map(static fn (\Flow\Parquet\Thrift\ColumnChunk $columnChunk) => ColumnChunk::fromThrift($columnChunk), $thrift->columns),
            $thrift->num_rows
        );
    }

    public function addColumnChunk(ColumnChunk $columnChunk) : void
    {
        $this->columnChunks[] = $columnChunk;
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

    public function setRowsCount(int $rowsCount) : void
    {
        if ($rowsCount < 0) {
            throw new InvalidArgumentException('Rows count must be greater than 0');
        }

        $this->rowsCount = $rowsCount;
    }

    public function toThrift() : \Flow\Parquet\Thrift\RowGroup
    {
        $fileOffset = \count($this->columnChunks) ? \current($this->columnChunks)->fileOffset() : 0;
        $chunksUncompressedSize = \array_map(static fn (ColumnChunk $chunk) => $chunk->totalUncompressedSize(), $this->columnChunks);
        $chunksCompressedSize = \array_map(static fn (ColumnChunk $chunk) => $chunk->totalCompressedSize(), $this->columnChunks);

        return new \Flow\Parquet\Thrift\RowGroup([
            'columns' => \array_map(static fn (ColumnChunk $columnChunk) => $columnChunk->toThrift(), $this->columnChunks),
            'num_rows' => $this->rowsCount,
            'file_offset' => $fileOffset,
            'total_byte_size' => \array_sum($chunksUncompressedSize),
            'total_compressed_size' => \array_sum($chunksCompressedSize),
        ]);
    }
}
