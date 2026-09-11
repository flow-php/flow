<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;

interface PartitioningLoader extends Loader
{
    public function partitionBy(Partitioning $partitioning): static;
}
