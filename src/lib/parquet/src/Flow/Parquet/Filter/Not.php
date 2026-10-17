<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

final readonly class Not implements Filter
{
    public function __construct(
        public Filter $filter,
    ) {}

    public function accept(FilterVisitor $visitor): mixed
    {
        return $visitor->not($this);
    }

    public function and(Filter $other): All
    {
        return new All($this, $other);
    }

    public function columns(): array
    {
        return $this->filter->columns();
    }

    public function negate(): Filter
    {
        return $this->filter;
    }

    public function or(Filter $other): Any
    {
        return new Any($this, $other);
    }
}
