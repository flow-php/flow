<?php

declare(strict_types=1);

namespace Flow\ETL\Partition;

use Flow\ETL\Partition;

final class NoopFilter implements PartitionFilter
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function keep(Partition ...$partitions) : bool
    {
        return true;
    }
}
