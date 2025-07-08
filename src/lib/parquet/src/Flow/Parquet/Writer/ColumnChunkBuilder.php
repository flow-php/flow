<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\ParquetFile\Schema\Column;

interface ColumnChunkBuilder
{
    public function addRow(WriteColumnData $columnData) : void;

    public function closePage() : void;

    public function column() : Column;

    /**
     * @return array<ColumnChunkContainer>
     */
    public function flush(int $fileOffset) : array;

    public function isFull() : bool;

    /**
     * Returns the uncompressed size of all pages in the column chunk.
     */
    public function uncompressedSize() : int;
}
