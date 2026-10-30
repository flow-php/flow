<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Binding\AbsentLeaf;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Binding\ExactLeaf;
use Flow\Parquet\Filter\Binding\InexactLeaf;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\Filter;
use Flow\Parquet\Filter\FilterVisitor;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Filter\Truth;
use Generator;

/**
 * @implements FilterVisitor<Truth>
 */
final readonly class StatisticsEvaluator implements FilterVisitor
{
    private function __construct(
        private BoundFilter $filter,
        private ZoneMaps $zones,
        private bool $negated = false,
    ) {}

    public static function evaluate(BoundFilter $filter, ZoneMaps $zones): Truth
    {
        return $filter->filter()->accept(new self($filter, $zones));
    }

    public function comparison(Comparison $comparison): Truth
    {
        return $this->membership($comparison, $comparison->operator === Operator::Equal);
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
        return $this->membership($in, true);
    }

    public function isNull(IsNull $isNull): Truth
    {
        $resolution = $this->filter->leaf($isNull);

        if ($resolution instanceof AbsentLeaf) {
            return $this->negated ? Truth::Never : Truth::Always;
        }

        $zone =
            $resolution instanceof ExactLeaf || $resolution instanceof InexactLeaf
                ? $this->zones->zone($resolution->column)
                : null;

        if ($zone === null) {
            return Truth::Maybe;
        }

        [$never, $always] = $this->negated
            ? [self::allNull($zone), $zone->nullCount === 0]
            : [$zone->nullCount === 0, self::allNull($zone)];

        return self::truth($never, $always);
    }

    public function not(Not $not): Truth
    {
        return $not->filter->accept(new self($this->filter, $this->zones, !$this->negated));
    }

    public function startsWith(StartsWith $startsWith): Truth
    {
        $resolution = $this->filter->leaf($startsWith);

        if ($resolution instanceof AbsentLeaf) {
            return Truth::Never;
        }

        if (!$resolution instanceof ExactLeaf) {
            return Truth::Maybe;
        }

        $intervals = $resolution->intervals;
        $zone = $this->zones->zone($resolution->column);

        if ($zone !== null && self::allNull($zone)) {
            return Truth::Never;
        }

        if ($zone === null || $zone->min === null || $zone->max === null) {
            return Truth::Maybe;
        }

        $none = $intervals->disjoint($zone->min, $zone->max);
        $all = $intervals->covers($zone->min, $zone->max);
        $noNull = $zone->nullCount === 0;

        return $this->negated ? self::truth($all, $noNull && $none) : self::truth($none, $noNull && $all);
    }

    /**
     * Comparison and In: the zone against the leaf's pre-image I.
     */
    private function membership(Comparison|In $node, bool $equality): Truth
    {
        $resolution = $this->filter->leaf($node);

        if ($resolution instanceof AbsentLeaf) {
            return Truth::Never;
        }

        if (!$resolution instanceof ExactLeaf) {
            return Truth::Maybe;
        }

        $intervals = $resolution->intervals;
        $zone = $this->zones->zone($resolution->column);

        if ($intervals->isEmpty()) {
            if (!$this->negated) {
                return Truth::Never;
            }

            return $zone === null ? Truth::Maybe : self::truth(self::allNull($zone), $zone->nullCount === 0);
        }

        if ($zone === null) {
            return Truth::Maybe;
        }

        if (self::allNull($zone)) {
            return Truth::Never;
        }

        $noNull = $zone->nullCount === 0;

        if ($zone->allNaN) {
            return $this->negated ? self::truth(false, $noNull) : Truth::Never;
        }

        if ($zone->min === null || $zone->max === null) {
            return Truth::Maybe;
        }

        $noNan = !$intervals->order->isFloatingPoint() || $zone->nanCount === 0;
        $none = $intervals->disjoint($zone->min, $zone->max);
        $all = (!$equality || $zone->minExact && $zone->maxExact) && $intervals->covers($zone->min, $zone->max);

        return $this->negated
            ? self::truth($noNan && $all, $noNull && $none)
            : self::truth($none, $noNull && $noNan && $all);
    }

    private static function allNull(ZoneMap $zone): bool
    {
        return $zone->nullCount === $zone->rowCount;
    }

    private static function truth(bool $never, bool $always): Truth
    {
        return match (true) {
            $never => Truth::Never,
            $always => Truth::Always,
            default => Truth::Maybe,
        };
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
