<?php declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Partition\FiltersCollection;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;

trait PartitionFiltering
{
    private ?PartitionFilter $partitionFilter = null;

    public function addPartitionFilter(PartitionFilter $filter) : void
    {
        if ($this->partitionFilter === null) {
            $this->partitionFilter = $filter;

            return;
        }

        if ($this->partitionFilter instanceof FiltersCollection) {
            $this->partitionFilter = new FiltersCollection([...$this->partitionFilter->filters, $filter]);

            return;
        }

        $this->partitionFilter = new FiltersCollection([$this->partitionFilter, $filter]);
    }

    public function partitionFilter() : PartitionFilter
    {
        return $this->partitionFilter ?? new NoopFilter();
    }
}
