<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

use DateTimeImmutable;

final readonly class Comparison implements Filter
{
    public bool|DateTimeImmutable|float|int|string $value;

    public function __construct(
        public ColumnRef $column,
        public Operator $operator,
        mixed $value,
    ) {
        $this->value = Literal::of($value);
    }

    public function accept(FilterVisitor $visitor): mixed
    {
        return $visitor->comparison($this);
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
