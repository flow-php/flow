<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Binding\AbsentLeaf;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Binding\ResolvedLeaf;
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
    private function __construct(
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
        $resolution = $this->filter->rowLeaf($isNull);

        if ($resolution instanceof AbsentLeaf) {
            return array_fill(0, $this->rows, !$this->negated);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($resolution) as $value) {
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
        $resolution = $this->filter->rowLeaf($startsWith);

        if ($resolution instanceof AbsentLeaf) {
            return array_fill(0, $this->rows, false);
        }

        $result = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($resolution) as $value) {
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
        $resolution = $this->filter->rowLeaf($node);

        if ($resolution instanceof AbsentLeaf) {
            return array_fill(0, $this->rows, false);
        }

        /** @var PhysicalIntervals $intervals rowLeaf() resolves only leaves answered exactly */
        $intervals = $resolution->intervals;
        $result = [];

        /** @var null|bool|float|int|string $value contains() refuses any other */
        foreach ($this->values($resolution) as $value) {
            $result[] = $value !== null && $intervals->contains($value) !== $this->negated;
        }

        return $result;
    }

    /**
     * @return list<mixed>
     */
    private function values(ResolvedLeaf $resolution): array
    {
        $path = $resolution->column->flatPath();

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
