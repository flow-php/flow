<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

final class ColumnPageHeader
{
    public function __construct(
        public readonly FlatColumn $column,
        public readonly PageHeader $pageHeader,
    ) {
    }
}
