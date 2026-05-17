<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\PlainValueUnpacker;
use Flow\Parquet\ParquetFile\Schema\ColumnPrimitiveType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final readonly class StatisticsReader
{
    private ByteOrder $byteOrder;

    public function __construct(
        private Statistics $statistics,
    ) {
        $this->byteOrder = ByteOrder::LITTLE_ENDIAN;
    }

    public function distinctCount(): ?int
    {
        return $this->statistics->distinctCount;
    }

    public function max(FlatColumn $column): mixed
    {
        $max = $this->statistics->max;

        if ($max === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $max;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($max), $this->byteOrder))->unpack(
            $column,
            1,
        ))[0];
    }

    public function maxValue(FlatColumn $column): mixed
    {
        $maxValue = $this->statistics->maxValue;

        if ($maxValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $maxValue;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($maxValue), $this->byteOrder))->unpack(
            $column,
            1,
        ))[0];
    }

    public function min(FlatColumn $column): mixed
    {
        $min = $this->statistics->min;

        if ($min === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $min;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($min), $this->byteOrder))->unpack(
            $column,
            1,
        ))[0];
    }

    public function minValue(FlatColumn $column): mixed
    {
        $minValue = $this->statistics->minValue;

        if ($minValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $minValue;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($minValue), $this->byteOrder))->unpack(
            $column,
            1,
        ))[0];
    }

    public function nullCount(): ?int
    {
        return $this->statistics->nullCount;
    }
}
