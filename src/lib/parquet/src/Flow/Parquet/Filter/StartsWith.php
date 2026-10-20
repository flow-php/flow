<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

final readonly class StartsWith implements Filter
{
    public function __construct(
        public ColumnRef $column,
        public string $prefix,
    ) {}

    public function accept(FilterVisitor $visitor): mixed
    {
        return $visitor->startsWith($this);
    }

    public function and(Filter $other): All
    {
        return new All($this, $other);
    }

    public function negate(): Filter
    {
        return new Not($this);
    }

    public function or(Filter $other): Any
    {
        return new Any($this, $other);
    }
}
