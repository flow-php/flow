<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\ParquetFile\Page\PageHeader;

final readonly class PageContainer
{
    /**
     * @param string $pageHeaderBuffer
     * @param string $pageBuffer
     * @param array $values - when dictionary is present values are indices
     * @param null|array $dictionary
     * @param PageHeader $pageHeader
     */
    public function __construct(
        public string $pageHeaderBuffer,
        public string $pageBuffer,
        public array $values,
        public ?array $dictionary,
        public PageHeader $pageHeader,
    ) {
    }

    public function dataSize() : int
    {
        return \strlen($this->pageBuffer);
    }

    public function headerSize() : int
    {
        return \strlen($this->pageHeaderBuffer);
    }

    public function totalCompressedSize() : int
    {
        return $this->headerSize() + $this->pageHeader->compressedPageSize();
    }

    public function totalUncompressedSize() : int
    {
        return $this->headerSize() + $this->pageHeader->uncompressedPageSize();
    }
}
