<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use function Flow\Parquet\array_flatten;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Dremel\Statistics\Comparator;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Data\PlainValuesPacker;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn};
use Flow\Parquet\ParquetFile\Statistics;

final class StatisticsCounter
{
    private readonly Comparator $comparator;

    private mixed $max;

    private mixed $min;

    private int $nullCount;

    private int $valuesCount;

    public function __construct(private readonly FlatColumn $column)
    {
        $this->nullCount = 0;
        $this->valuesCount = 0;
        $this->min = null;
        $this->max = null;
        $this->comparator = new Comparator();
    }

    /**
     * @param null|array<null|bool|float|int|object|string>|bool|float|int|object|string $value
     */
    public function add(string|int|float|array|bool|object|null $value) : void
    {
        if (\is_array($value)) {
            $value = array_flatten($value);
        }

        if (\is_array($value)) {
            $arrayValuesCount = \count($value);
            $this->valuesCount += $arrayValuesCount ?: 1;
        } else {
            $this->valuesCount++;
        }

        if ($value === null) {
            $this->nullCount++;

            return;
        }

        if (\is_array($value)) {
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

    public function max() : mixed
    {
        return $this->max;
    }

    public function merge(self $statistics) : self
    {
        if ($this->column !== $statistics->column) {
            throw new InvalidArgumentException('Cannot merge statistics for different columns.');
        }

        $newStatistics = new self($this->column);

        $newStatistics->nullCount = $this->nullCount + $statistics->nullCount;
        $newStatistics->valuesCount = $this->valuesCount + $statistics->valuesCount;
        $newStatistics->min = $this->comparator->isLessThan($this->min, $statistics->min) ? $this->min : $statistics->min;
        $newStatistics->max = $this->comparator->isGreaterThan($this->max, $statistics->max) ? $this->max : $statistics->max;

        return $newStatistics;
    }

    public function min() : mixed
    {
        return $this->min;
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
        $this->valuesCount = 0;
        $this->min = null;
        $this->max = null;
    }

    public function toStatistics() : Statistics
    {
        $minBuffer = '';
        $maxBuffer = '';

        (new PlainValuesPacker(new BinaryBufferWriter($minBuffer)))->packValues($this->column, [$this->min()]);
        (new PlainValuesPacker(new BinaryBufferWriter($maxBuffer)))->packValues($this->column, [$this->max()]);

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

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
