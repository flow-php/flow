<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\Page\PageHeader;

final class DataPageContainer
{
    public function __construct(
        public readonly string $pageHeaderBuffer,
        public readonly string $dataBuffer,
        public readonly PageHeader $pageHeader
    ) {
    }
}
