<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\Thrift\{CompactProtocol, MemoryBuffer};

final class PageContainer
{
    private ?string $serializedHeader = null;

    /**
     * @param string $compressedData - Compressed page data (repetition levels, definition levels, and values)
     * @param PageHeader $pageHeader
     */
    public function __construct(
        public string $compressedData,
        public PageHeader $pageHeader,
    ) {
    }

    public function dataSize() : int
    {
        return \strlen($this->compressedData);
    }

    public function headerSize() : int
    {
        return \strlen($this->serializedHeader());
    }

    public function serializedHeader() : string
    {
        if ($this->serializedHeader !== null) {
            return $this->serializedHeader;
        }

        $this->pageHeader->toThrift()->write(new CompactProtocol($buffer = new MemoryBuffer()));

        $this->serializedHeader = $buffer->data();

        return $this->serializedHeader;
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
