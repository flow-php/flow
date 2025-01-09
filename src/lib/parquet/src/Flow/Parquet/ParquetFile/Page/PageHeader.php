<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader, Type};
use Flow\Parquet\{Options};

final readonly class PageHeader
{
    public function __construct(
        private Type $type,
        private int $compressedPageSize,
        private int $uncompressedPageSize,
        private ?DataPageHeader $dataPageHeader,
        private ?DataPageHeaderV2 $dataPageHeaderV2,
        private ?DictionaryPageHeader $dictionaryPageHeader,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\PageHeader $thrift, Options $options) : self
    {
        return new self(
            Type::from($thrift->type),
            $thrift->compressed_page_size,
            $thrift->uncompressed_page_size,
            $thrift->data_page_header !== null ? DataPageHeader::fromThrift($thrift->data_page_header) : null,
            $thrift->data_page_header_v2 !== null ? DataPageHeaderV2::fromThrift($thrift->data_page_header_v2, $options) : null,
            $thrift->dictionary_page_header !== null ? DictionaryPageHeader::fromThrift($thrift->dictionary_page_header) : null
        );
    }

    public function compressedPageSize() : int
    {
        return $this->compressedPageSize;
    }

    public function dataPageHeader() : ?DataPageHeader
    {
        return $this->dataPageHeader;
    }

    public function dataPageHeaderV2() : ?DataPageHeaderV2
    {
        return $this->dataPageHeaderV2;
    }

    public function dataValuesCount() : ?int
    {
        if ($this->dataPageHeader !== null) {
            return $this->dataPageHeader->valuesCount();
        }

        if ($this->dataPageHeaderV2 !== null) {
            return $this->dataPageHeaderV2->valuesCount();
        }

        return null;
    }

    public function dictionaryPageHeader() : ?DictionaryPageHeader
    {
        return $this->dictionaryPageHeader;
    }

    public function dictionaryValuesCount() : ?int
    {
        if ($this->dictionaryPageHeader !== null) {
            return $this->dictionaryPageHeader->valuesCount();
        }

        return null;
    }

    public function encoding() : Encodings
    {
        if ($this->dictionaryPageHeader) {
            return $this->dictionaryPageHeader->encoding();
        }

        if ($this->dataPageHeaderV2) {
            return $this->dataPageHeaderV2->encoding();
        }

        /**
         * @phpstan-ignore-next-line
         */
        return $this->dataPageHeader->encoding();
    }

    public function toThrift() : \Flow\Parquet\Thrift\PageHeader
    {
        return new \Flow\Parquet\Thrift\PageHeader([
            'type' => $this->type->value,
            'compressed_page_size' => $this->compressedPageSize,
            'uncompressed_page_size' => $this->uncompressedPageSize,
            'crc' => null,
            'data_page_header' => $this->dataPageHeader?->toThrift(),
            'data_page_header_v2' => $this->dataPageHeaderV2?->toThrift(),
            'dictionary_page_header' => $this->dictionaryPageHeader?->toThrift(),
            'index_page_header' => null,
        ]);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function uncompressedPageSize() : int
    {
        return $this->uncompressedPageSize;
    }
}
