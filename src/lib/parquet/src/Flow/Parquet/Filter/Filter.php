<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

/**
 * The implementations are closed: All, Any, Comparison, In, IsNull, Not and StartsWith; every walker refuses another.
 */
interface Filter
{
    /**
     * @template T
     *
     * @param FilterVisitor<T> $visitor
     *
     * @return T
     */
    public function accept(FilterVisitor $visitor): mixed;

    public function and(self $other): All;

    public function negate(): self;

    public function or(self $other): Any;
}
