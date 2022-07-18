<?php

declare(strict_types=1);

namespace Flow\ETL;

final class PartitionedRows
{
    /**
     * @var array<Partition>
     */
    public readonly array $partitions;

    public function __construct(public readonly Rows $rows, Partition $partition, Partition ...$partitions)
    {
        \array_unshift($partitions, $partition);

        $this->partitions = $partitions;
    }
}
