<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

use DateTimeImmutable;

use function array_map;
use function array_values;

final readonly class In implements Filter
{
    /**
     * @var list<bool|DateTimeImmutable|float|int|string>
     */
    public array $values;

    public function __construct(
        public ColumnRef $column,
        mixed ...$values,
    ) {
        $this->values = array_map(Literal::of(...), array_values($values));
    }

    public function accept(FilterVisitor $visitor): mixed
    {
        return $visitor->in($this);
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
