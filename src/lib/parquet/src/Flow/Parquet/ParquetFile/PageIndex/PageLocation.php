<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\PageIndex;

use Flow\Parquet\ThriftModel\PageLocation as ThriftPageLocation;

/**
 * $offset and $compressedPageSize include the page header; $firstRowIndex is relative to the row group.
 */
final readonly class PageLocation
{
    public function __construct(
        public int $offset,
        public int $compressedPageSize,
        public int $firstRowIndex,
    ) {}

    public static function fromThrift(ThriftPageLocation $location): self
    {
        return new self(
            (int) $location->offset,
            (int) $location->compressed_page_size,
            (int) $location->first_row_index,
        );
    }

    public function toThrift(): ThriftPageLocation
    {
        return new ThriftPageLocation([
            'offset' => $this->offset,
            'compressed_page_size' => $this->compressedPageSize,
            'first_row_index' => $this->firstRowIndex,
        ]);
    }
}
