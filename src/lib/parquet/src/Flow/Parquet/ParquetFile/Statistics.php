<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

final class Statistics
{
    public function __construct(
        public readonly ?string $max,
        public readonly ?string $min,
        public readonly ?int $nullCount,
        public readonly ?int $distinctCount,
        public readonly ?string $maxValue,
        public readonly ?string $minValue,
    ) {

    }

    public static function fromThrift(\Flow\Parquet\Thrift\Statistics $thrift) : self
    {
        return new self(
            $thrift->max,
            $thrift->min,
            $thrift->null_count,
            $thrift->distinct_count,
            $thrift->max_value,
            $thrift->min_value,
        );
    }

    public function toThrift() : \Flow\Parquet\Thrift\Statistics
    {
        return new \Flow\Parquet\Thrift\Statistics([
            'max' => $this->max,
            'min' => $this->min,
            'null_count' => $this->nullCount,
            'distinct_count' => $this->distinctCount,
            'max_value' => $this->maxValue,
            'min_value' => $this->minValue,
        ]);
    }
}
