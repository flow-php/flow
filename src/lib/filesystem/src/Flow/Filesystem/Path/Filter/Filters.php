<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path\Filter;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter;

use function array_merge;
use function array_values;

final readonly class Filters implements Filter
{
    /**
     * @var array<Filter>
     */
    private array $filters;

    public function __construct(Filter ...$filters)
    {
        $this->filters = $filters;
    }

    public function accept(FileStatus $status): bool
    {
        foreach ($this->filters as $filter) {
            if (!$filter->accept($status)) {
                return false;
            }
        }

        return true;
    }

    public function add(Filter $filter): self
    {
        return new self(...array_merge($this->filters, [$filter]));
    }

    /**
     * @return list<Filter>
     */
    public function filters(): array
    {
        return array_values($this->filters);
    }
}
