<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Page\Header;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\RowGroup\StatisticsReader;
use Flow\Parquet\ParquetFile\Statistics;

final readonly class DataPageHeaderV2
{
    public function __construct(
        private int $valuesCount,
        private int $nullsCount,
        private int $rowsCount,
        private Encodings $encoding,
        private int $definitionsByteLength,
        private int $repetitionsByteLength,
        private ?bool $isCompressed,
        private ?Statistics $statistics,
    ) {}

    public static function fromThrift(\Flow\Parquet\ThriftModel\DataPageHeaderV2 $thrift, Options $options): self
    {
        return new self(
            (int) $thrift->num_values,
            (int) $thrift->num_nulls,
            (int) $thrift->num_rows,
            Encodings::from($thrift->encoding),
            (int) $thrift->definition_levels_byte_length,
            (int) $thrift->repetition_levels_byte_length,
            /** @phpstan-ignore-next-line */
            $thrift->is_compressed ?? null,
            $thrift->statistics ? Statistics::fromThrift($thrift->statistics) : null,
        );
    }

    public function definitionsByteLength(): int
    {
        return $this->definitionsByteLength;
    }

    public function encoding(): Encodings
    {
        return $this->encoding;
    }

    public function repetitionsByteLength(): int
    {
        return $this->repetitionsByteLength;
    }

    public function statistics(Options $options): ?StatisticsReader
    {
        if ($this->statistics === null) {
            return null;
        }

        return new StatisticsReader($this->statistics);
    }

    public function toThrift(): \Flow\Parquet\ThriftModel\DataPageHeaderV2
    {
        return new \Flow\Parquet\ThriftModel\DataPageHeaderV2([
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

    public function valuesCount(): int
    {
        return $this->valuesCount;
    }
}
