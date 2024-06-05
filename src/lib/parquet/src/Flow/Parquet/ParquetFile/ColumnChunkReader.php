<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Stream;

interface ColumnChunkReader
{
    /**
     * @return \Generator<array<mixed>>
     */
    public function read(ColumnChunk $columnChunk, FlatColumn $column, Stream $stream) : \Generator;
}
