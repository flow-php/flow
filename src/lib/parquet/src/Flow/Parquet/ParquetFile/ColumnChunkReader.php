<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\Column;

interface ColumnChunkReader
{
    /**
     * @param resource $stream
     */
    public function read(ColumnChunk $columnChunk, Column $column, $stream, int $limit = null) : \Generator;
}
