<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\{Filters, OnlyFiles};

trait PathFiltering
{
    private ?Filter $filter = null;

    public function addFilter(Filter $filter) : void
    {
        if ($this->filter === null) {
            $this->filter = $filter;

            return;
        }

        if ($this->filter instanceof Filters) {
            $this->filter = $this->filter->add($filter);

            return;
        }

        $this->filter = new Filters($this->filter, $filter);
    }

    public function filter() : Filter
    {
        return $this->filter ?? new OnlyFiles();
    }
}
