<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

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

    /**
     * @return list<ColumnRef>
     */
    public function columns(): array;

    public function negate(): self;

    public function or(self $other): Any;
}
