<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final readonly class ColumnPageHeader
{
    public function __construct(
        public FlatColumn $column,
        public ColumnChunk $columnChunk,
        public PageHeader $pageHeader,
    ) {
    }
}
