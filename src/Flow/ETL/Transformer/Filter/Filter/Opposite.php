<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Filter\Filter;

use Flow\ETL\Row;
use Flow\ETL\Transformer\Filter\Filter;

/**
 * @implements Filter<array{filter: Filter}>
 * @psalm-immutable
 */
final class Opposite implements Filter
{
    /**
     * @var Filter
     */
    private Filter $filter;

    public function __construct(Filter $filter)
    {
        $this->filter = $filter;
    }

    public function __serialize() : array
    {
        return [
            'filter' => $this->filter,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->filter = $data['filter'];
    }

    public function keep(Row $row) : bool
    {
        return !$this->filter->keep($row);
    }
}
