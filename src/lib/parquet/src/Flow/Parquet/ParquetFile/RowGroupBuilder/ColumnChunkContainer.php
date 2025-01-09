<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

final readonly class ColumnChunkContainer
{
    public function __construct(
        public string $binaryBuffer,
        public ColumnChunk $columnChunk,
    ) {
    }
}
