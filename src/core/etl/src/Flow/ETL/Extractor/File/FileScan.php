<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Conjuncts;
use Flow\ETL\Function\ScalarFunction;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function min;

final readonly class FileScan
{
    /**
     * @var null|int<1, max>
     */
    public ?int $limit;

    /**
     * @param null|int $limit the rows the plan needs, see Extractor::extract()
     * @param Filter $pathFilter what this read lists
     * @param list<ScalarFunction> $conjuncts predicates over the source's columns, in push order; the source returns
     *                                        only rows matching every one, and $limit counts those rows
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function __construct(
        ?int $limit = null,
        public Filter $pathFilter = new OnlyFiles(),
        public array $conjuncts = [],
    ) {
        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        $this->limit = $limit;
    }

    public function narrows(): bool
    {
        return $this->limit !== null || !$this->pathFilter instanceof OnlyFiles || $this->conjuncts !== [];
    }

    public function withFilter(ScalarFunction $filter): self
    {
        return new self($this->limit, $this->pathFilter, [...$this->conjuncts, ...(new Conjuncts())->of($filter)]);
    }

    /**
     * Narrowing only: a second push may lower the limit, never raise it.
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function withLimit(int $limit): self
    {
        return new self(
            $this->limit === null ? $limit : min($this->limit, $limit),
            $this->pathFilter,
            $this->conjuncts,
        );
    }

    public function withPathFilter(Filter $filter): self
    {
        return new self(
            $this->limit,
            $this->pathFilter instanceof Filters
                ? $this->pathFilter->add($filter)
                : new Filters($this->pathFilter, $filter),
            $this->conjuncts,
        );
    }
}
