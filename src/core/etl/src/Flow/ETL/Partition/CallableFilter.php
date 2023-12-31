<?php

declare(strict_types=1);

namespace Flow\ETL\Partition;

use Flow\ETL\Partition;

/**
 * @deprecated please use ScalarFunctionFilter instead
 */
final class CallableFilter implements PartitionFilter
{
    /**
     * @var callable(Partition ...$partition) : bool
     */
    private $filter;

    /**
     * @param callable(Partition ...$partition) : bool $filter
     */
    public function __construct(callable $filter)
    {
        $this->filter = $filter;
    }

    public function keep(Partition ...$partitions) : bool
    {
        return ($this->filter)(...$partitions);
    }
}
