<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Conjuncts;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function min;

final readonly class Criteria
{
    /**
     * @var null|int<1, max>
     */
    public ?int $limit;

    /**
     * @var int<0, max>
     */
    public int $offset;

    /**
     * @param null|int $limit the rows the plan needs after $offset, see Extractor::extract()
     * @param int $offset the rows the source skips before it yields, counted like $limit; unlike $limit it is binding,
     *                    and only a FileExtractor ever receives one
     * @param Filter $pathFilter what this read lists
     * @param list<ScalarFunction> $conjuncts predicates over the source's columns, in push order; the source returns
     *                                        only rows matching every one, and $limit and $offset count those rows
     * @param null|FileSplit $split the piece of the source this read covers; only a SplittableExtractor, which cut it,
     *                              ever receives one, and a read with a split lists nothing
     *
     * @throws InvalidArgumentException when $limit is not greater than 0, when $offset is below 0, or with both an
     *                                  offset and a split
     */
    public function __construct(
        ?int $limit = null,
        int $offset = 0,
        public Filter $pathFilter = new OnlyFiles(),
        public array $conjuncts = [],
        public ?FileSplit $split = null,
    ) {
        if ($limit !== null && $limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0, given: ' . $offset);
        }

        // an offset counts across the whole source, which a split does not see
        if ($offset > 0 && $split !== null) {
            throw new InvalidArgumentException('A read with an offset cannot be split, given offset: ' . $offset);
        }

        $this->limit = $limit;
        $this->offset = $offset;
    }

    public function narrows(): bool
    {
        return (
            $this->limit !== null
            || $this->offset > 0
            || !$this->pathFilter instanceof OnlyFiles
            || $this->conjuncts !== []
            || $this->split !== null
        );
    }

    public function withFilter(ScalarFunction $filter): self
    {
        return new self(
            $this->limit,
            $this->offset,
            $this->pathFilter,
            [...$this->conjuncts, ...(new Conjuncts())->of($filter)],
            $this->split,
        );
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
            $this->offset,
            $this->pathFilter,
            $this->conjuncts,
            $this->split,
        );
    }

    /**
     * Adds to the offset already pushed. The limit stays: it bounds the rows the plan needs, and skipping more only
     * lowers that.
     *
     * @throws InvalidArgumentException when $offset is below 0, or when this read is split
     */
    public function withOffset(int $offset): self
    {
        if ($offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0, given: ' . $offset);
        }

        return new self($this->limit, $this->offset + $offset, $this->pathFilter, $this->conjuncts, $this->split);
    }

    public function withPathFilter(Filter $filter): self
    {
        return new self(
            $this->limit,
            $this->offset,
            $this->pathFilter instanceof Filters
                ? $this->pathFilter->add($filter)
                : new Filters($this->pathFilter, $filter),
            $this->conjuncts,
            $this->split,
        );
    }

    /**
     * @throws InvalidArgumentException when this read has an offset
     */
    public function withSplit(FileSplit $split): self
    {
        return new self($this->limit, $this->offset, $this->pathFilter, $this->conjuncts, $split);
    }
}
