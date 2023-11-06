<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\Data\ObjectToString;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Statistics\Comparator;

final class DataPageV2Statistics
{
    private Comparator $comparator;

    private mixed $max;

    private mixed $min;

    private int $nullCount;

    private array $values = [];

    private int $valuesCount;

    public function __construct()
    {
        $this->nullCount = 0;
        $this->valuesCount = 0;
        $this->min = null;
        $this->max = null;
        $this->comparator = new Comparator();
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

        if (\is_array($value)) {
            foreach ($value as $val) {

                if ($this->comparator->isLessThan($val, $this->min)) {
                    $this->min = $val;
                }

                if ($this->comparator->isGreaterThan($val, $this->max)) {
                    $this->max = $val;
                }

                $this->values[] = \is_object($val) ? ObjectToString::toString($val) : $val;
            }
        } else {
            if ($this->comparator->isLessThan($value, $this->min)) {
                $this->min = $value;
            }

            if ($this->comparator->isGreaterThan($value, $this->max)) {
                $this->max = $value;
            }

            $this->values[] = \is_object($value) ? ObjectToString::toString($value) : $value;
        }
    }

    public function distinctCount() : int
    {
        if ([] === $this->values) {
            return 0;
        }

        return \count(\array_unique($this->values));
    }

    public function max() : mixed
    {
        return $this->max;
    }

    public function min() : mixed
    {
        return $this->min;
    }

    public function nullCount() : int
    {
        return $this->nullCount;
    }

    public function values() : array
    {
        return $this->values;
    }

    public function valuesCount() : int
    {
        return $this->valuesCount;
    }
}
