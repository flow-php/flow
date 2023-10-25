<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\Page\PageHeader;

final class PageContainer
{
    public function __construct(
        public readonly string $pageHeaderBuffer,
        public readonly string $pageBuffer,
        public readonly array $values,
        public readonly PageHeader $pageHeader
    ) {
    }

    public function size() : int
    {
        return \strlen($this->pageHeaderBuffer) + \strlen($this->pageBuffer);
    }
}
