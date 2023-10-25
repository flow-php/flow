<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

interface ColumnChunkViewer
{
    /**
     * @param resource $stream
     */
    public function view(ColumnChunk $columnChunk, FlatColumn $column, $stream) : \Generator;
}
