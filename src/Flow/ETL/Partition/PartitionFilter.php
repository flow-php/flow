<?php

declare(strict_types=1);

namespace Flow\ETL\Partition;

use Flow\ETL\Partition;

interface PartitionFilter
{
    public function keep(Partition ...$partitions) : bool;
}
