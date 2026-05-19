<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\ThriftModel\Statistics as ThriftStatistics;

final readonly class Statistics
{
    public function __construct(
        public ?string $max,
        public ?string $min,
        public ?int $nullCount,
        public ?int $distinctCount,
        public ?string $maxValue,
        public ?string $minValue,
        public ?bool $isMaxValueExact = null,
        public ?bool $isMinValueExact = null,
    ) {}

    public static function fromThrift(ThriftStatistics $thrift): self
    {
        return new self(
            $thrift->max,
            $thrift->min,
            // @mago-ignore analysis:redundant-condition
            // @mago-ignore analysis:redundant-comparison
            $thrift->null_count !== null ? (int) $thrift->null_count : null,
            // @mago-ignore analysis:redundant-condition
            // @mago-ignore analysis:redundant-comparison
            $thrift->distinct_count !== null ? (int) $thrift->distinct_count : null,
            $thrift->max_value,
            $thrift->min_value,
            $thrift->is_max_value_exact,
            $thrift->is_min_value_exact,
        );
    }

    public function toThrift(): ThriftStatistics
    {
        return new ThriftStatistics([
            'max' => $this->max,
            'min' => $this->min,
            'null_count' => $this->nullCount,
            'distinct_count' => $this->distinctCount,
            'max_value' => $this->maxValue,
            'min_value' => $this->minValue,
            'is_max_value_exact' => $this->isMaxValueExact,
            'is_min_value_exact' => $this->isMinValueExact,
        ]);
    }
}
