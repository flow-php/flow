<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;

trait PathFiltering
{
    private ?Filter $filter = null;

    /**
     * Which partition columns a read discovers is a function of the path and this filter, so the
     * listing is cached next to the filter that invalidates it: one listing per extractor instance,
     * however often schema() is asked.
     *
     * @var null|array<string, bool>
     */
    private ?array $partitionNames = null;

    public function filter(): Filter
    {
        return $this->filter ?? new OnlyFiles();
    }

    /**
     * @return array<string, bool>
     */
    public function partitionNames(PartitionColumns $partitionColumns, Path $path): array
    {
        return $this->partitionNames ??= $partitionColumns->names($path, $this->filter());
    }

    public function withPathFilter(Filter $filter): static
    {
        $this->partitionNames = null;

        if ($this->filter === null) {
            $this->filter = $filter;

            return $this;
        }

        if ($this->filter instanceof Filters) {
            $this->filter = $this->filter->add($filter);

            return $this;
        }

        $this->filter = new Filters($this->filter, $filter);

        return $this;
    }
}
