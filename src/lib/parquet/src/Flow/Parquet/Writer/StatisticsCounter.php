<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\PlainValuesPacker;
use Flow\Parquet\Dremel\Statistics\Comparator;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Statistics;

use function count;
use function Flow\Parquet\array_flatten;
use function is_array;
use function is_string;

final class StatisticsCounter
{
    private readonly ByteOrder $byteOrder;

    private readonly Comparator $comparator;

    private mixed $max;

    private mixed $min;

    private int $nullCount;

    private int $valuesCount;

    public function __construct(
        private readonly FlatColumn $column,
    ) {
        $this->nullCount = 0;
        $this->valuesCount = 0;
        $this->min = null;
        $this->max = null;
        $this->comparator = new Comparator();
        $this->byteOrder = ByteOrder::LITTLE_ENDIAN;
    }

    /**
     * @param null|array<null|bool|float|int|object|string>|bool|float|int|object|string $value
     */
    public function add(string|int|float|array|bool|object|null $value): void
    {
        if (is_array($value)) {
            $value = array_flatten($value);
        }

        if (is_array($value)) {
            $arrayValuesCount = count($value);
            $this->valuesCount += $arrayValuesCount ?: 1;
        } else {
            $this->valuesCount++;
        }

        if ($value === null) {
            $this->nullCount++;

            return;
        }

        if (is_array($value)) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $val) {
                if ($this->comparator->isLessThan($val, $this->min)) {
                    $this->min = $val;
                }

                if ($this->comparator->isGreaterThan($val, $this->max)) {
                    $this->max = $val;
                }
            }
        } else {
            if ($this->comparator->isLessThan($value, $this->min)) {
                $this->min = $value;
            }

            if ($this->comparator->isGreaterThan($value, $this->max)) {
                $this->max = $value;
            }
        }
    }

    /**
     * @param array<null|bool|float|int|object|string> $values
     */
    public function addBatch(array $values): void
    {
        $this->valuesCount += count($values);

        foreach ($values as $value) {
            if ($value === null) {
                $this->nullCount++;

                continue;
            }

            if ($this->comparator->isLessThan($value, $this->min)) {
                $this->min = $value;
            }

            if ($this->comparator->isGreaterThan($value, $this->max)) {
                $this->max = $value;
            }
        }
    }

    public function addNulls(int $count): void
    {
        if ($count < 0) {
            throw new InvalidArgumentException('Null count cannot be negative.');
        }

        $this->nullCount += $count;
        $this->valuesCount += $count;
    }

    public function max(): mixed
    {
        return $this->max;
    }

    public function merge(self $statistics): self
    {
        if ($this->column !== $statistics->column) {
            throw new InvalidArgumentException('Cannot merge statistics for different columns.');
        }

        $newStatistics = new self($this->column);

        $newStatistics->nullCount = $this->nullCount + $statistics->nullCount;
        $newStatistics->valuesCount = $this->valuesCount + $statistics->valuesCount;
        $newStatistics->min = $this->comparator->isLessThan($this->min, $statistics->min)
            ? $this->min
            : $statistics->min;
        $newStatistics->max = $this->comparator->isGreaterThan($this->max, $statistics->max)
            ? $this->max
            : $statistics->max;

        return $newStatistics;
    }

    public function min(): mixed
    {
        return $this->min;
    }

    public function notNullCount(): int
    {
        return $this->valuesCount - $this->nullCount;
    }

    public function nullCount(): int
    {
        return $this->nullCount;
    }

    public function reset(): void
    {
        $this->nullCount = 0;
        $this->valuesCount = 0;
        $this->min = null;
        $this->max = null;
    }

    public function toStatistics(): Statistics
    {
        $minBuffer = '';
        $maxBuffer = '';

        // @mago-ignore analysis:mixed-assignment
        $min = $this->min();
        // @mago-ignore analysis:mixed-assignment
        $max = $this->max();

        if ($min !== null) {
            if ($this->column->type() === PhysicalType::BYTE_ARRAY && is_string($min)) {
                (new BinaryBufferWriter($minBuffer))->append($min);
            } else {
                (new PlainValuesPacker(new BinaryBufferWriter($minBuffer), $this->byteOrder))->packValues(
                    $this->column,
                    [$min],
                );
            }
        }

        if ($max !== null) {
            if ($this->column->type() === PhysicalType::BYTE_ARRAY && is_string($max)) {
                (new BinaryBufferWriter($maxBuffer))->append($max);
            } else {
                (new PlainValuesPacker(new BinaryBufferWriter($maxBuffer), $this->byteOrder))->packValues(
                    $this->column,
                    [$max],
                );
            }
        }

        return new Statistics(
            max: $maxBuffer !== '' ? $maxBuffer : null,
            min: $minBuffer !== '' ? $minBuffer : null,
            nullCount: $this->nullCount(),
            distinctCount: null,
            maxValue: $maxBuffer !== '' ? $maxBuffer : null,
            minValue: $minBuffer !== '' ? $minBuffer : null,
            isMaxValueExact: null,
            isMinValueExact: null,
        );
    }

    public function valuesCount(): int
    {
        return $this->valuesCount;
    }
}
