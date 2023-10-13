<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\Encodings;

/**
 * @psalm-suppress RedundantConditionGivenDocblockType
 * @psalm-suppress RedundantCastGivenDocblockType
 */
final class DataPageHeader
{
    public function __construct(
        private readonly Encodings $encoding,
        private readonly int $valuesCount,
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\DataPageHeader $thrift) : self
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

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
