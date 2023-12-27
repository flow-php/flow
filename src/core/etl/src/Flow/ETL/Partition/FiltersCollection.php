<?php declare(strict_types=1);

namespace Flow\ETL\Partition;

use Flow\ETL\Partition;

/**
 * @implements PartitionFilter<array{filters: array<PartitionFilter>}>
 */
final class FiltersCollection implements PartitionFilter
{
    /**
     * @param array<PartitionFilter> $filters
     */
    public function __construct(public readonly array $filters)
    {

    }

    public function __serialize() : array
    {
        return ['filters' => $this->filters];
    }

    public function __unserialize(array $data) : void
    {
        $this->filters = $data['filters'];
    }

    public function keep(Partition ...$partitions) : bool
    {
        foreach ($this->filters as $filter) {
            if (!$filter->keep(...$partitions)) {
                return false;
            }
        }

        return true;
    }
}
