<?php declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;

trait PartitionFiltering
{
    private ?PartitionFilter $partitionFilter = null;

    public function partitionFilter() : PartitionFilter
    {
        return $this->partitionFilter ?? new NoopFilter();
    }

    public function setPartitionFilter(PartitionFilter $filter) : void
    {
        $this->partitionFilter = $filter;
    }
}
