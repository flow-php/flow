<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Binding\Absent;
use Flow\Parquet\Filter\Binding\Bound;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\FilterVisitor;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\StartsWith;

use function array_fill;
use function array_key_exists;
use function count;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function sprintf;
use function str_starts_with;

/**
 * The rows a filter is TRUE for, on undecoded physical values: floats compare as IEEE 754 (NaN is false, -0.0 equals
 * +0.0), every other type in its physical order. Not is pushed down to the leaves as a polarity, so a NULL is TRUE in
 * neither.
 *
 * @implements FilterVisitor<list<bool>>
 */
final readonly class RowEvaluator implements FilterVisitor
{
    /**
     * @param array<string, list<mixed>> $physicalByPath one physical value or null per row, keyed by FlatColumn::flatPath()
     * @param non-negative-int $rows
     */
    public function __construct(
        private BoundFilter $filter,
        private array $physicalByPath,
        private int $rows,
        private bool $negated = false,
    ) {}

    /**
     * @param array<string, list<mixed>> $physicalByPath one physical value or null per row, keyed by FlatColumn::flatPath()
     * @param non-negative-int $rows
     *
     * @return list<bool> true = keep
     */
    public static function evaluate(BoundFilter $filter, array $physicalByPath, int $rows): array
    {
        return $filter->filter()->accept(new self($filter, $physicalByPath, $rows));
    }

    public function all(All $all): array
    {
        return $this->combined($all->filters, !$this->negated);
    }

    public function any(Any $any): array
    {
        return $this->combined($any->filters, $this->negated);
    }

    public function comparison(Comparison $comparison): array
    {
        return $this->membership($comparison);
    }

    public function in(In $in): array
    {
        return $this->membership($in);
    }

    public function isNull(IsNull $isNull): array
    {
        $leaf = $this->filter->bound($isNull);

        if ($leaf instanceof Absent) {
            return array_fill(0, $this->rows, !$this->negated);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($leaf) as $value) {
            $result[] = ($value === null) !== $this->negated;
        }

        return $result;
    }

    public function not(Not $not): array
    {
        return $not->filter->accept(new self($this->filter, $this->physicalByPath, $this->rows, !$this->negated));
    }

    public function startsWith(StartsWith $startsWith): array
    {
        $leaf = $this->filter->bound($startsWith);

        if ($leaf instanceof Absent) {
            return array_fill(0, $this->rows, false);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($leaf) as $value) {
            $result[] = is_string($value) && str_starts_with($value, $startsWith->prefix) !== $this->negated;
        }

        return $result;
    }

    /**
     * Each row TRUE in every filter when $conjunction, in any otherwise.
     *
     * @param list<Filter> $filters
     *
     * @return list<bool>
     */
    private function combined(array $filters, bool $conjunction): array
    {
        $result = array_fill(0, $this->rows, $conjunction);

        foreach ($filters as $filter) {
            foreach ($filter->accept($this) as $row => $truth) {
                if ($truth !== $conjunction) {
                    $result[$row] = $truth;
                }
            }
        }

        return $result;
    }

    /**
     * A non-null value is in the leaf's pre-image.
     *
     * @return list<bool>
     */
    private function membership(Comparison|In $node): array
    {
        $leaf = $this->filter->bound($node);

        if ($leaf instanceof Absent) {
            return array_fill(0, $this->rows, false);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($leaf) as $value) {
            if ($value === null) {
                $result[] = false;

                continue;
            }

            if (!is_bool($value) && !is_float($value) && !is_int($value) && !is_string($value)) {
                throw new InvalidArgumentException(sprintf(
                    'Column "%s" holds a value that is no physical value',
                    $leaf->column->flatPath(),
                ));
            }

            $result[] = $leaf->intervals->contains($leaf->order, $value) !== $this->negated;
        }

        return $result;
    }

    /**
     * @return list<mixed>
     */
    private function values(Bound $leaf): array
    {
        $path = $leaf->column->flatPath();

        if (!array_key_exists($path, $this->physicalByPath)) {
            throw new InvalidArgumentException('No physical values of column "' . $path . '"');
        }

        $values = $this->physicalByPath[$path];

        if (count($values) !== $this->rows) {
            throw new InvalidArgumentException(sprintf(
                'Column "%s" holds %d physical values for %d rows',
                $path,
                count($values),
                $this->rows,
            ));
        }

        return $values;
    }
}
