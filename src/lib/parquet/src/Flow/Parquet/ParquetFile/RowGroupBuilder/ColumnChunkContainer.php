<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

final class ColumnChunkContainer
{
    public function __construct(
        public readonly string $binaryBuffer,
        public readonly ColumnChunk $columnChunk
    ) {
    }
}
