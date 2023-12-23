<?php declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Partition\PartitionFilter;

interface PartitionsExtractor
{
    public function partitionFilter() : ?PartitionFilter;

    public function setPartitionFilter(PartitionFilter $filter) : void;
}
