<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;

final readonly class ColumnChunkContainer
{
    public function __construct(
        public string $binaryBuffer,
        public ColumnChunk $columnChunk,
        public ?ColumnIndex $columnIndex,
        public OffsetIndex $offsetIndex,
        public ?Sbbf $bloomFilter = null,
    ) {}
}
