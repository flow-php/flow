<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\{Filters, OnlyFiles};

trait PathFiltering
{
    private ?Filter $filter = null;

    /**
     * @deprecated Use withPathFilter instead
     */
    public function addFilter(Filter $filter) : self
    {
        return $this->withPathFilter($filter);
    }

    public function filter() : Filter
    {
        return $this->filter ?? new OnlyFiles();
    }

    public function withPathFilter(Filter $filter) : self
    {
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
