<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

use function array_values;

final readonly class All implements Filter
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
        return $visitor->all($this);
    }

    public function and(Filter $other): self
    {
        return new self($this, $other);
    }

    public function columns(): array
    {
        $columns = [];

        foreach ($this->filters as $filter) {
            foreach ($filter->columns() as $column) {
                $columns[$column->path] = $column;
            }
        }

        return array_values($columns);
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
