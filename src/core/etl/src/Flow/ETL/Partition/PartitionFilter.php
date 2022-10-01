<?php

declare(strict_types=1);

namespace Flow\ETL\Partition;

use Flow\ETL\Partition;
use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface PartitionFilter extends Serializable
{
    public function keep(Partition ...$partitions) : bool;
}
