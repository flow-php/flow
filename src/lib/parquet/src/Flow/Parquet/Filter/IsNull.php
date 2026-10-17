<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

final readonly class IsNull implements Filter
{
    public function __construct(
        public ColumnRef $column,
    ) {}

    public function accept(FilterVisitor $visitor): mixed
    {
        return $visitor->isNull($this);
    }

    public function and(Filter $other): All
    {
        return new All($this, $other);
    }

    public function columns(): array
    {
        return [$this->column];
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
