<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

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
use Flow\Parquet\Filter\Operator;
use Flow\Parquet\Filter\StartsWith;
use Flow\Parquet\Filter\Truth;
use Generator;

/**
 * @implements FilterVisitor<Truth>
 */
final readonly class StatisticsEvaluator implements FilterVisitor
{
    public function __construct(
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
        $leaf = $this->filter->leaf($isNull);

        if ($leaf instanceof Absent) {
            return $this->negated ? Truth::Never : Truth::Always;
        }

        $zone = $leaf instanceof Bound ? $this->zones->zone($leaf->column) : null;

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
        $leaf = $this->filter->leaf($startsWith);

        if ($leaf instanceof Absent) {
            return Truth::Never;
        }

        $zone = $leaf instanceof Bound ? $this->zones->zone($leaf->column) : null;

        if ($zone !== null && self::allNull($zone)) {
            return Truth::Never;
        }

        if (!$leaf instanceof Bound || $zone === null || $zone->min === null || $zone->max === null) {
            return Truth::Maybe;
        }

        $none = $leaf->intervals->disjoint($leaf->order, $zone->min, $zone->max);
        $all = $leaf->intervals->covers($leaf->order, $zone->min, $zone->max);
        $noNull = $zone->nullCount === 0;

        return $this->negated ? self::truth($all, $noNull && $none) : self::truth($none, $noNull && $all);
    }

    /**
     * Comparison and In: the zone against the leaf's pre-image I.
     */
    private function membership(Comparison|In $leaf, bool $equality): Truth
    {
        $bound = $this->filter->leaf($leaf);

        if ($bound instanceof Absent) {
            return Truth::Never;
        }

        if (!$bound instanceof Bound) {
            return Truth::Maybe;
        }

        $zone = $this->zones->zone($bound->column);

        if ($bound->intervals->isEmpty()) {
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

        $noNan = !$bound->order->isFloatingPoint() || $zone->nanCount === 0;
        $none = $bound->intervals->disjoint($bound->order, $zone->min, $zone->max);
        $all =
            (!$equality || $zone->minExact && $zone->maxExact)
            && $bound->intervals->covers($bound->order, $zone->min, $zone->max);

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
