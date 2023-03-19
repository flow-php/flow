<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Filter\Filter;

/**
 * @implements Transformer<array{filters: array<Filter>}>
 */
final class FilterRowsTransformer implements Transformer
{
    /**
     * @var Filter[]
     */
    private readonly array $filters;

    public function __construct(Filter ...$filters)
    {
        $this->filters = $filters;
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

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $filter = function (Row $row) : bool {
            foreach ($this->filters as $filter) {
                if (false === $filter->keep($row)) {
                    return false;
                }
            }

            return true;
        };

        return $rows->filter($filter);
    }
}
