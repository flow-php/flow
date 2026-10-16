<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Data\PhysicalCodec;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;

use function count;
use function fdiv;
use function is_nan;
use function is_object;
use function strcmp;

final class StatisticsCounter
{
    /**
     * Both bounds are NaN until the first non-NaN value: arrow-rs get_min_max replaces min and max together.
     */
    private bool $boundsAreNan = false;

    private readonly PhysicalCodec $codec;

    private readonly bool $floatingPoint;

    private bool $bounded = false;

    private bool|float|int|string $max = 0;

    private int|string $maxKey = 0;

    private bool|float|int|string $min = 0;

    private int|string $minKey = 0;

    private ?int $nanCount = null;

    private int $nullCount = 0;

    private readonly PhysicalOrder $order;

    private readonly bool $signed;

    private int $valuesCount = 0;

    public function __construct(
        private readonly FlatColumn $column,
        private readonly StatisticsTruncation $truncation,
    ) {
        $this->order = PhysicalOrder::forColumn($column);
        $this->codec = new PhysicalCodec();
        $this->signed = $this->order->isSigned();
        $this->floatingPoint = $this->order->isFloatingPoint();
    }

    /**
     * @param array<null|bool|float|int|object|string> $values
     */
    public function addBatch(array $values): void
    {
        $this->valuesCount += count($values);
        $physical = [];

        foreach ($values as $value) {
            if ($value === null) {
                $this->nullCount++;
            } elseif (is_object($value)) {
                throw new InvalidArgumentException(
                    'Column "' . $this->column->flatPath() . '" statistics take physical values, got ' . $value::class,
                );
            } else {
                $physical[] = $value;
            }
        }

        if ($physical === []) {
            return;
        }

        match ($this->order) {
            PhysicalOrder::Signed, PhysicalOrder::Boolean => $this->addNative($physical),
            PhysicalOrder::Bytes => $this->addBytes($physical),
            PhysicalOrder::Double, PhysicalOrder::Float => $this->addFloats($physical),
            PhysicalOrder::Float16 => $this->addFloat16($physical),
            PhysicalOrder::Decimal, PhysicalOrder::Unsigned32, PhysicalOrder::Unsigned64 => $this->addByKey($physical),
            PhysicalOrder::Unordered => null,
        };
    }

    public function addNulls(int $count): void
    {
        if ($count < 0) {
            throw new InvalidArgumentException('Null count cannot be negative.');
        }

        $this->nullCount += $count;
        $this->valuesCount += $count;
    }

    public function max(): bool|float|int|string|null
    {
        return $this->bounded ? $this->max : null;
    }

    public function merge(self $statistics): self
    {
        if ($this->column !== $statistics->column) {
            throw new InvalidArgumentException('Cannot merge statistics for different columns.');
        }

        $merged = new self($this->column, $this->truncation);

        foreach ([$this, $statistics] as $part) {
            if ($part->bounded) {
                $merged->addBatch([$part->min, $part->max]);
            }
        }

        $merged->nullCount = $this->nullCount + $statistics->nullCount;
        $merged->valuesCount = $this->valuesCount + $statistics->valuesCount;
        $merged->nanCount =
            $this->nanCount === null && $statistics->nanCount === null
                ? null
                : (int) $this->nanCount + (int) $statistics->nanCount;

        return $merged;
    }

    public function min(): bool|float|int|string|null
    {
        return $this->bounded ? $this->min : null;
    }

    public function nanCount(): ?int
    {
        return $this->nanCount;
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
        $this->nanCount = null;
        $this->bounded = false;
        $this->boundsAreNan = false;
    }

    /**
     * Data page statistics: a float page always carries a nan_count, 0 for an all-null page (arrow-rs get_nan_count).
     */
    public function toPageStatistics(): Statistics
    {
        return $this->statistics($this->floatingPoint ? (int) $this->nanCount : $this->nanCount);
    }

    public function toStatistics(): Statistics
    {
        return $this->statistics($this->nanCount);
    }

    private function statistics(?int $nanCount): Statistics
    {
        if (!$this->bounded) {
            return new Statistics(
                max: null,
                min: null,
                nullCount: $this->nullCount,
                distinctCount: null,
                maxValue: null,
                minValue: null,
                isMaxValueExact: false,
                isMinValueExact: false,
                nanCount: $nanCount,
            );
        }

        $min = $this->codec->encodeStatistic($this->column, $this->min);
        $max = $this->codec->encodeStatistic($this->column, $this->max);
        $minValue = $this->truncation->min($this->column, $min);
        $maxValue = $this->truncation->max($this->column, $max);

        return new Statistics(
            max: $this->signed ? $maxValue : null,
            min: $this->signed ? $minValue : null,
            nullCount: $this->nullCount,
            distinctCount: null,
            maxValue: $maxValue,
            minValue: $minValue,
            isMaxValueExact: $maxValue === $max,
            isMinValueExact: $minValue === $min,
            nanCount: $nanCount,
        );
    }

    public function valuesCount(): int
    {
        return $this->valuesCount;
    }

    /**
     * @param non-empty-list<bool|float|int|string> $values
     */
    private function addByKey(array $values): void
    {
        foreach ($values as $value) {
            $this->bound($value, $this->order->key($value), false);
        }
    }

    /**
     * @param non-empty-list<bool|float|int|string> $values
     */
    private function addBytes(array $values): void
    {
        foreach ($values as $value) {
            $value = (string) $value;

            if (!$this->bounded) {
                $this->min = $value;
                $this->max = $value;
                $this->bounded = true;
            } elseif (strcmp($value, (string) $this->min) < 0) {
                $this->min = $value;
            } elseif (strcmp($value, (string) $this->max) > 0) {
                $this->max = $value;
            }
        }
    }

    /**
     * @param non-empty-list<bool|float|int|string> $values
     */
    private function addFloat16(array $values): void
    {
        $nanCount = (int) $this->nanCount;

        foreach ($values as $value) {
            $nan = $this->order->isNan($value);
            $nanCount += $nan ? 1 : 0;
            $this->bound($value, $this->order->key($value), $nan);
        }

        $this->nanCount = $nanCount;
    }

    /**
     * Native float order is the IEEE 754 total order except for -0.0/+0.0 ties and NaN, handled on their own.
     *
     * @param non-empty-list<bool|float|int|string> $values
     */
    private function addFloats(array $values): void
    {
        $this->nanCount = (int) $this->nanCount;

        foreach ($values as $value) {
            $value = (float) $value;

            if (is_nan($value)) {
                $this->nanCount++;

                if (!$this->bounded) {
                    $this->min = $value;
                    $this->max = $value;
                    $this->bounded = true;
                    $this->boundsAreNan = true;
                } elseif ($this->boundsAreNan && $this->order->compare($value, $this->min) < 0) {
                    $this->min = $value;
                } elseif ($this->boundsAreNan && $this->order->compare($value, $this->max) > 0) {
                    $this->max = $value;
                }

                continue;
            }

            if (!$this->bounded || $this->boundsAreNan) {
                $this->min = $value;
                $this->max = $value;
                $this->bounded = true;
                $this->boundsAreNan = false;

                continue;
            }

            if (
                $value < $this->min
                || $value === 0.0 && $this->min === 0.0 && fdiv(1.0, $value) < fdiv(1.0, $this->min)
            ) {
                $this->min = $value;
            } elseif (
                $value > $this->max
                || $value === 0.0 && $this->max === 0.0 && fdiv(1.0, $value) > fdiv(1.0, $this->max)
            ) {
                $this->max = $value;
            }
        }
    }

    /**
     * @param non-empty-list<bool|float|int|string> $values
     */
    private function addNative(array $values): void
    {
        foreach ($values as $value) {
            if (!$this->bounded) {
                $this->min = $value;
                $this->max = $value;
                $this->bounded = true;
            } elseif ($value < $this->min) {
                $this->min = $value;
            } elseif ($value > $this->max) {
                $this->max = $value;
            }
        }
    }

    /**
     * The key-ordered paths (addByKey, addFloat16) keep minKey/maxKey; a NaN bound gives way to the first non-NaN
     * value and never replaces one (arrow-rs get_min_max).
     */
    private function bound(bool|float|int|string $value, int|string $key, bool $nan): void
    {
        if (!$this->bounded || $this->boundsAreNan && !$nan) {
            $this->min = $value;
            $this->minKey = $key;
            $this->max = $value;
            $this->maxKey = $key;
            $this->bounded = true;
            $this->boundsAreNan = $nan;

            return;
        }

        if ($this->boundsAreNan !== $nan) {
            return;
        }

        if ($this->order->compareKeys($key, $this->minKey) < 0) {
            $this->min = $value;
            $this->minKey = $key;
        } elseif ($this->order->compareKeys($key, $this->maxKey) > 0) {
            $this->max = $value;
            $this->maxKey = $key;
        }
    }
}
