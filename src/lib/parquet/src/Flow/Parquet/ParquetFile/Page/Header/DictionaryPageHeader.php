<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ThriftModel\DictionaryPageHeader as ThriftDictionaryPageHeader;

final readonly class DictionaryPageHeader
{
    public function __construct(
        private Encodings $encoding,
        private int $valuesCount,
    ) {}

    public static function fromThrift(ThriftDictionaryPageHeader $thrift): self
    {
        return new self(Encodings::from($thrift->encoding), (int) $thrift->num_values);
    }

    public function encoding(): Encodings
    {
        return $this->encoding;
    }

    public function toThrift(): ThriftDictionaryPageHeader
    {
        return new ThriftDictionaryPageHeader([
            'encoding' => $this->encoding->value,
            'num_values' => $this->valuesCount,
            'is_sorted' => false,
        ]);
    }

    public function valuesCount(): int
    {
        return $this->valuesCount;
    }
}
