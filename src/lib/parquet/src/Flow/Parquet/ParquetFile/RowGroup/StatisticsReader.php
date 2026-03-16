<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\PlainValueUnpacker;
use Flow\Parquet\ParquetFile\Schema\{ColumnPrimitiveType, FlatColumn};
use Flow\Parquet\ParquetFile\Statistics;

final readonly class StatisticsReader
{
    private ByteOrder $byteOrder;

    public function __construct(private Statistics $statistics)
    {
        $this->byteOrder = ByteOrder::LITTLE_ENDIAN;
    }

    public function distinctCount() : ?int
    {
        return $this->statistics->distinctCount;
    }

    public function max(FlatColumn $column) : mixed
    {
        if ($this->statistics->max === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $this->statistics->max;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($this->statistics->max), $this->byteOrder))->unpack($column, 1))[0];
    }

    public function maxValue(FlatColumn $column) : mixed
    {
        if ($this->statistics->maxValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $this->statistics->maxValue;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($this->statistics->maxValue), $this->byteOrder))->unpack($column, 1))[0];
    }

    public function min(FlatColumn $column) : mixed
    {
        if ($this->statistics->min === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $this->statistics->min;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($this->statistics->min), $this->byteOrder))->unpack($column, 1))[0];
    }

    public function minValue(FlatColumn $column) : mixed
    {
        if ($this->statistics->minValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column)) {
            return $this->statistics->minValue;
        }

        return \iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($this->statistics->minValue), $this->byteOrder))->unpack($column, 1))[0];
    }

    public function nullCount() : ?int
    {
        return $this->statistics->nullCount;
    }
}
