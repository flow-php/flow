<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;

/**
 * @psalm-suppress RedundantConditionGivenDocblockType
 */
final class PageHeader
{
    public function __construct(
        private readonly Type $type,
        private readonly int $compressedPageSize,
        private readonly int $uncompressedPageSize,
        private readonly ?DataPageHeader $dataPageHeader,
        private readonly ?DataPageHeaderV2 $dataPageHeaderV2,
        private readonly ?DictionaryPageHeader $dictionaryPageHeader
    ) {
    }

    /**
     * @psalm-suppress DocblockTypeContradiction
     */
    public static function fromThrift(\Flow\Parquet\Thrift\PageHeader $thrift) : self
    {
        return new self(
            Type::from($thrift->type),
            $thrift->compressed_page_size,
            $thrift->uncompressed_page_size,
            $thrift->data_page_header !== null ? DataPageHeader::fromThrift($thrift->data_page_header) : null,
            $thrift->data_page_header_v2 !== null ? DataPageHeaderV2::fromThrift($thrift->data_page_header_v2) : null,
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
         * @psalm-suppress PossiblyNullReference
         *
         * @phpstan-ignore-next-line
         */
        return $this->dataPageHeader->encoding();
    }

    public function normalize() : array
    {
        return [
            'type' => $this->type()->name,
            'dictionary_values_count' => $this->dictionaryValuesCount(),
            'data_values_count' => $this->dataValuesCount(),
            'encoding' => $this->encoding()->name,
            'compressed_page_size' => $this->compressedPageSize(),
        ];
    }

    public function toThrift() : \Flow\Parquet\Thrift\PageHeader
    {
        return new \Flow\Parquet\Thrift\PageHeader([
            'type' => $this->type->value,
            'compressed_page_size' => $this->compressedPageSize,
            'uncompressed_page_size' => $this->uncompressedPageSize,
            'crc' => null,
            'data_page_header' => $this->dataPageHeader?->toThrift(),
            'data_page_header_v2' => null,
            'dictionary_page_header' => null,
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
