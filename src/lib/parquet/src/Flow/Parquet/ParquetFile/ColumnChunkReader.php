<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface ColumnChunkReader
{
    /**
     * @return \Generator<FlatColumnValues>
     */
    public function read(ColumnChunk $columnChunk, FlatColumn $column, SourceStream $stream) : \Generator;
}
