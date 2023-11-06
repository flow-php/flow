<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\RowGroup\StatisticsReader;
use Flow\Parquet\ParquetFile\Statistics;

/**
 * @psalm-suppress RedundantConditionGivenDocblockType
 * @psalm-suppress RedundantCastGivenDocblockType
 */
final class DataPageHeaderV2
{
    public function __construct(
        private readonly int $valuesCount,
        private readonly int $nullsCount,
        private readonly int $rowsCount,
        private readonly Encodings $encoding,
        private readonly int $definitionsByteLength,
        private readonly int $repetitionsByteLength,
        private readonly ?bool $isCompressed,
        private readonly ?Statistics $statistics
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\DataPageHeaderV2 $thrift) : self
    {
        /** @psalm-suppress DocblockTypeContradiction */
        return new self(
            $thrift->num_values,
            $thrift->num_nulls,
            $thrift->num_rows,
            Encodings::from($thrift->encoding),
            $thrift->definition_levels_byte_length,
            $thrift->repetition_levels_byte_length,
            /** @phpstan-ignore-next-line */
            $thrift->is_compressed ?? null,
            /** @phpstan-ignore-next-line */
            $thrift->statistics ? Statistics::fromThrift($thrift->statistics) : null
        );
    }

    public function definitionsByteLength() : int
    {
        return $this->definitionsByteLength;
    }

    public function encoding() : Encodings
    {
        return $this->encoding;
    }

    public function repetitionsByteLength() : int
    {
        return $this->repetitionsByteLength;
    }

    public function statistics() : ?StatisticsReader
    {
        if ($this->statistics === null) {
            return null;
        }

        return new StatisticsReader($this->statistics);
    }

    public function toThrift() : \Flow\Parquet\Thrift\DataPageHeaderV2
    {
        return new \Flow\Parquet\Thrift\DataPageHeaderV2([
            'num_values' => $this->valuesCount,
            'num_nulls' => $this->nullsCount,
            'num_rows' => $this->rowsCount,
            'definition_levels_byte_length' => $this->definitionsByteLength,
            'repetition_levels_byte_length' => $this->repetitionsByteLength,
            'encoding' => $this->encoding->value,
            'is_compressed' => $this->isCompressed,
            'statistics' => $this->statistics?->toThrift(),
        ]);
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
