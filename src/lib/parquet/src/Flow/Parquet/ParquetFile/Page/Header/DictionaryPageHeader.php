<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\Encodings;

/**
 * @psalm-suppress RedundantConditionGivenDocblockType
 * @psalm-suppress RedundantCastGivenDocblockType
 */
final class DictionaryPageHeader
{
    public function __construct(
        private readonly Encodings $encoding,
        private readonly int $valuesCount,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\DictionaryPageHeader $thrift) : self
    {
        return new self(
            Encodings::from($thrift->encoding),
            $thrift->num_values
        );
    }

    public function encoding() : Encodings
    {
        return $this->encoding;
    }

    public function toThrift() : \Flow\Parquet\Thrift\DictionaryPageHeader
    {
        return new \Flow\Parquet\Thrift\DictionaryPageHeader([
            'encoding' => $this->encoding->value,
            'num_values' => $this->valuesCount,
            'is_sorted' => false,
        ]);
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
