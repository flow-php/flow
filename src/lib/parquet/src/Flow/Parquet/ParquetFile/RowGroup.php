<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ThriftModel\ColumnChunk as ThriftColumnChunk;
use Flow\Parquet\ThriftModel\RowGroup as ThriftRowGroup;

use function array_map;
use function array_sum;
use function current;

final class RowGroup
{
    /**
     * @param array<ColumnChunk> $columnChunks
     * @param int $rowsCount
     */
    public function __construct(
        private array $columnChunks,
        private int $rowsCount,
    ) {}

    public static function fromThrift(ThriftRowGroup $thrift): self
    {
        return new self(
            array_map(static fn(ThriftColumnChunk $columnChunk) => ColumnChunk::fromThrift(
                $columnChunk,
            ), $thrift->columns),
            (int) $thrift->num_rows,
        );
    }

    public function addColumnChunk(ColumnChunk $columnChunk): void
    {
        $this->columnChunks[] = $columnChunk;
    }

    /**
     * @return array<ColumnChunk>
     */
    public function columnChunks(): array
    {
        return $this->columnChunks;
    }

    public function getColumnChunk(FlatColumn $column): ColumnChunk
    {
        foreach ($this->columnChunks as $chunk) {
            if ($chunk->flatPath() === $column->flatPath()) {
                return $chunk;
            }
        }

        throw new InvalidArgumentException(
            "Column chunk '{$column->flatPath()}' not found in row group, when looking for chunks for NestedColumns, look for chunks for each child.",
        );
    }

    public function rowsCount(): int
    {
        return $this->rowsCount;
    }

    public function setRowsCount(int $rowsCount): void
    {
        if ($rowsCount < 0) {
            throw new InvalidArgumentException('Rows count must be greater than 0');
        }

        $this->rowsCount = $rowsCount;
    }

    public function totalByteSize(): int
    {
        return array_sum(array_map(
            static fn(ColumnChunk $chunk) => $chunk->totalUncompressedSize(),
            $this->columnChunks,
        ));
    }

    public function toThrift(): ThriftRowGroup
    {
        $firstChunk = current($this->columnChunks);
        $fileOffset = $firstChunk !== false ? $firstChunk->fileOffset() : 0;
        $chunksUncompressedSize = array_map(
            static fn(ColumnChunk $chunk) => $chunk->totalUncompressedSize(),
            $this->columnChunks,
        );
        $chunksCompressedSize = array_map(
            static fn(ColumnChunk $chunk) => $chunk->totalCompressedSize(),
            $this->columnChunks,
        );

        return new ThriftRowGroup([
            'columns' => array_map(
                static fn(ColumnChunk $columnChunk) => $columnChunk->toThrift(),
                $this->columnChunks,
            ),
            'num_rows' => $this->rowsCount,
            'file_offset' => $fileOffset,
            'total_byte_size' => array_sum($chunksUncompressedSize),
            'total_compressed_size' => array_sum($chunksCompressedSize),
        ]);
    }
}
