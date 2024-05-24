<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Partition\PartitionFilter;

interface PartitionExtractor
{
    public function addPartitionFilter(PartitionFilter $filter) : void;

    public function partitionFilter() : ?PartitionFilter;
}
