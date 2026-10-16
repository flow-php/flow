<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\Data\PhysicalCodec;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final readonly class StatisticsReader
{
    private PhysicalCodec $codec;

    public function __construct(
        private Statistics $statistics,
    ) {
        $this->codec = new PhysicalCodec();
    }

    public function distinctCount(): ?int
    {
        return $this->statistics->distinctCount;
    }

    public function isMaxValueExact(): ?bool
    {
        return $this->statistics->isMaxValueExact;
    }

    public function isMinValueExact(): ?bool
    {
        return $this->statistics->isMinValueExact;
    }

    public function max(FlatColumn $column): bool|float|int|string|null
    {
        return $this->decode($column, $this->statistics->max);
    }

    public function maxValue(FlatColumn $column): bool|float|int|string|null
    {
        return $this->decode($column, $this->statistics->maxValue);
    }

    public function min(FlatColumn $column): bool|float|int|string|null
    {
        return $this->decode($column, $this->statistics->min);
    }

    public function minValue(FlatColumn $column): bool|float|int|string|null
    {
        return $this->decode($column, $this->statistics->minValue);
    }

    public function nanCount(): ?int
    {
        return $this->statistics->nanCount;
    }

    public function nullCount(): ?int
    {
        return $this->statistics->nullCount;
    }

    private function decode(FlatColumn $column, ?string $bytes): bool|float|int|string|null
    {
        return $bytes === null ? null : $this->codec->decodeStatistic($column, $bytes);
    }
}
