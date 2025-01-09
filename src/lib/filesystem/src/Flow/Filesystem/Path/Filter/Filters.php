<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path\Filter;

use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\{FileStatus};

final readonly class Filters implements Filter
{
    /**
     * @param array<Filter> $filters
     */
    private array $filters;

    public function __construct(Filter ...$filters)
    {
        $this->filters = $filters;
    }

    public function accept(FileStatus $status) : bool
    {
        foreach ($this->filters as $filter) {
            if (!$filter->accept($status)) {
                return false;
            }
        }

        return true;
    }

    public function add(Filter $filter) : self
    {
        return new self(...\array_merge($this->filters, [$filter]));
    }
}
