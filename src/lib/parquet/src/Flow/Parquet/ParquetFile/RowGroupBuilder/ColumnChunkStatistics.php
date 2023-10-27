<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\ColumnPrimitiveType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class ColumnChunkStatistics
{
    private int $distinctCount;

    private int $nullCount;

    private int $totalStringLength;

    private int $valuesCount;

    public function __construct(private readonly FlatColumn $column)
    {
        $this->nullCount = 0;
        $this->distinctCount = 0;
        $this->valuesCount = 0;
        $this->totalStringLength = 0;
    }

    public function add(string|int|float|null|array|bool|object $value) : void
    {
        if (\is_array($value)) {
            $this->valuesCount += \count($value);
        } else {
            $this->valuesCount++;
        }

        if ($value === null) {
            $this->nullCount++;

            return;
        }

        if ((\is_string($value) || \is_array($value)) && ColumnPrimitiveType::isString($this->column)) {
            if (\is_string($value)) {
                $this->totalStringLength += \strlen($value);
            }

            if (\is_array($value)) {
                foreach ($value as $v) {
                    if (\is_string($v)) {
                        $this->totalStringLength += \strlen($v);
                    }
                }
            }
        }
    }

    public function avgStringLength() : int
    {
        return (int) \ceil($this->totalStringLength / $this->notNullCount());
    }

    public function distinctCount() : int
    {
        return $this->distinctCount;
    }

    public function notNullCount() : int
    {
        return $this->valuesCount - $this->nullCount;
    }

    public function nullCount() : int
    {
        return $this->nullCount;
    }

    public function reset() : void
    {
        $this->nullCount = 0;
        $this->distinctCount = 0;
        $this->valuesCount = 0;
        $this->totalStringLength = 0;
    }

    public function totalStringLength() : int
    {
        return $this->totalStringLength;
    }

    public function uncompressedSize() : int
    {
        switch ($this->column->type()) {
            case PhysicalType::BOOLEAN:
                // Booleans are stored as bits, so we can fit 8 of them into a single byte
                return (int) \ceil($this->notNullCount() / 8);
            case PhysicalType::FLOAT:
            case PhysicalType::INT32:
                // Int32s are stored as 4 bytes
                return $this->notNullCount() * 4;
            case PhysicalType::DOUBLE:
            case PhysicalType::INT64:
                // Int64s are stored as 8 bytes
                return $this->notNullCount() * 8;
            case PhysicalType::INT96:
                // Int96s are stored as 12 bytes
                return $this->notNullCount() * 12;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                // Fixed length byte arrays are stored as their length
                return $this->notNullCount() * $this->column->typeLength();
            case PhysicalType::BYTE_ARRAY:
                return $this->totalStringLength() + (4 * $this->notNullCount()); // each string starts with int32 length
        }

        throw new RuntimeException('Unknown column type');
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
