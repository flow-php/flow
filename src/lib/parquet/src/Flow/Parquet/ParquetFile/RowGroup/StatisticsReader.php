<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroup;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ParquetFile\Data\PlainValueUnpacker;
use Flow\Parquet\ParquetFile\Schema\ColumnPrimitiveType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

final class StatisticsReader
{
    public function __construct(private readonly Statistics $statistics)
    {
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

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->max, 'UTF-8')) {
            return $this->statistics->max;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->max))))->unpack($column, 1)[0];
    }

    public function maxValue(FlatColumn $column) : mixed
    {
        if ($this->statistics->maxValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->maxValue, 'UTF-8')) {
            return $this->statistics->maxValue;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->maxValue))))->unpack($column, 1)[0];
    }

    public function min(FlatColumn $column) : mixed
    {
        if ($this->statistics->min === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->min, 'UTF-8')) {
            return $this->statistics->min;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->min))))->unpack($column, 1)[0];
    }

    public function minValue(FlatColumn $column) : mixed
    {
        if ($this->statistics->minValue === null) {
            return null;
        }

        if (ColumnPrimitiveType::isString($column) && \mb_check_encoding($this->statistics->minValue, 'UTF-8')) {
            return $this->statistics->minValue;
        }

        return (new PlainValueUnpacker((new BinaryBufferReader($this->statistics->minValue))))->unpack($column, 1)[0];
    }

    public function nullCount() : ?int
    {
        return $this->statistics->nullCount;
    }
}
