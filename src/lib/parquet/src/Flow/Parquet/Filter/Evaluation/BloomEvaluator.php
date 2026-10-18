<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Binding\Bound;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\FilterVisitor;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Filter\Truth;
use Flow\Parquet\ParquetFile\Index\Indexes;
use Generator;

use function is_float;

/**
 * A bloom filter proves only absence: a positive Equal or In leaf whose literals are all absent is Never, every other
 * leaf is Maybe. A row group's filter is read only for such a leaf.
 *
 * @implements FilterVisitor<Truth>
 */
final readonly class BloomEvaluator implements FilterVisitor
{
    public function __construct(
        private BoundFilter $filter,
        private Indexes $indexes,
        private int $rowGroup,
        private bool $negated = false,
    ) {}

    public static function evaluate(BoundFilter $filter, Indexes $indexes, int $rowGroup): Truth
    {
        return $filter->filter()->accept(new self($filter, $indexes, $rowGroup));
    }

    public function comparison(Comparison $comparison): Truth
    {
        return $comparison->operator === Operator::Equal ? $this->membership($comparison) : Truth::Maybe;
    }

    public function all(All $all): Truth
    {
        return $this->negated ? Truth::some($this->truths($all->filters)) : Truth::every($this->truths($all->filters));
    }

    public function any(Any $any): Truth
    {
        return $this->negated ? Truth::every($this->truths($any->filters)) : Truth::some($this->truths($any->filters));
    }

    public function in(In $in): Truth
    {
        return $this->membership($in);
    }

    public function isNull(IsNull $isNull): Truth
    {
        return Truth::Maybe;
    }

    public function not(Not $not): Truth
    {
        return $not->filter->accept(new self($this->filter, $this->indexes, $this->rowGroup, !$this->negated));
    }

    public function startsWith(StartsWith $startsWith): Truth
    {
        return Truth::Maybe;
    }

    /**
     * Never when no point of the leaf's pre-image is in the filter. A zero is checked as both -0.0 and +0.0, which
     * compare equal but hash apart.
     */
    private function membership(Comparison|In $leaf): Truth
    {
        $bound = $this->filter->leaf($leaf);

        if ($this->negated || !$bound instanceof Bound) {
            return Truth::Maybe;
        }

        $points = [];

        foreach ($bound->intervals->intervals as $interval) {
            if (
                $interval->lower === null
                || $interval->lower !== $interval->upper
                || !$interval->lowerInclusive
                || !$interval->upperInclusive
            ) {
                return Truth::Maybe;
            }

            $points[] = $interval->lower;

            if (is_float($interval->lower) && $interval->lower === 0.0) {
                $points[] = -$interval->lower;
            }
        }

        if ($points === []) {
            return Truth::Never;
        }

        $bloomFilter = $this->indexes->bloomFilter($this->rowGroup, $bound->column);

        if ($bloomFilter === null) {
            return Truth::Maybe;
        }

        $hash = new BloomHash();

        foreach ($points as $point) {
            if ($bloomFilter->check($hash->of($bound->column, $point))) {
                return Truth::Maybe;
            }
        }

        return Truth::Never;
    }

    /**
     * Each filter's truth, evaluated only when the combinator asks for it.
     *
     * @param list<Filter> $filters
     *
     * @return Generator<Truth>
     */
    private function truths(array $filters): Generator
    {
        foreach ($filters as $filter) {
            yield $filter->accept($this);
        }
    }
}
