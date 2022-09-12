<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter;

/**
 * @implements Filter<array{filters: array<Filter>}>
 *
 * @psalm-immutable
 */
final class Any implements Filter
{
    /**
     * @var Filter[]
     */
    private array $filters;

    public function __construct(Filter ...$filter)
    {
        $this->filters = $filter;
    }

    public function __serialize() : array
    {
        return [
            'filters' => $this->filters,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->filters = $data['filters'];
    }

    public function keep(Row $row) : bool
    {
        foreach ($this->filters as $filter) {
            if ($filter->keep($row)) {
                return true;
            }
        }

        return false;
    }
}
