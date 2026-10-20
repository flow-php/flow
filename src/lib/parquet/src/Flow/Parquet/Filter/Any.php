<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

use function array_values;

final readonly class Any implements Filter
{
    /**
     * @var list<Filter>
     */
    public array $filters;

    public function __construct(Filter ...$filters)
    {
        $this->filters = array_values($filters);
    }

    public function accept(FilterVisitor $visitor): mixed
    {
        return $visitor->any($this);
    }

    public function and(Filter $other): All
    {
        return new All($this, $other);
    }

    public function negate(): Filter
    {
        return new Not($this);
    }

    public function or(Filter $other): self
    {
        return new self($this, $other);
    }
}
