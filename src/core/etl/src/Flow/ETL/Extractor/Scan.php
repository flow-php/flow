<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function min;

/**
 * The read-time parameters of one plan Read: what the planner pushed into the source. The source
 * keeps no state about them, so no plan ever has to copy an extractor.
 */
final readonly class Scan
{
    /**
     * @param null|int<1, max> $limit rows in total, across every file or page the read touches
     * @param Filter $pathFilter what a FileExtractor lists for this read
     */
    public function __construct(
        public ?int $limit = null,
        public Filter $pathFilter = new OnlyFiles(),
    ) {}

    /**
     * Narrowing only: a second push may lower the cap, never raise it.
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function withLimit(int $limit): self
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        return new self($this->limit === null ? $limit : min($this->limit, $limit), $this->pathFilter);
    }

    public function withPathFilter(Filter $filter): self
    {
        return new self(
            $this->limit,
            $this->pathFilter instanceof Filters
                ? $this->pathFilter->add($filter)
                : new Filters($this->pathFilter, $filter),
        );
    }
}
