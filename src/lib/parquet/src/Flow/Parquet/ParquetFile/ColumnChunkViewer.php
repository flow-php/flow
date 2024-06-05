<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Stream;

interface ColumnChunkViewer
{
    /**
     * @param resource $stream
     */
    public function view(ColumnChunk $columnChunk, FlatColumn $column, Stream $stream) : \Generator;
}
