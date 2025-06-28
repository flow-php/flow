<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface ColumnChunkReader
{
    /**
     * @return \Generator<WriteFlatColumnValues>
     */
    public function read(ColumnChunk $columnChunk, FlatColumn $column, SourceStream $stream) : \Generator;
}
