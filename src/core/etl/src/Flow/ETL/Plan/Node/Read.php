<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Function\All;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Schema;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\Filters;

use function min;

final readonly class Read implements Node
{
    /**
     * @param Extractor $extractor the instance the user handed to read(); the planner never mutates or copies it
     * @param FileScan $scan what the plan pushed into the read, handed to FileExtractor::scan() when the plan
     *                       runs; a non-file extractor receives only its limit
     */
    public function __construct(
        private Extractor $extractor,
        private FileScan $scan = new FileScan(),
    ) {}

    public function extractor(): Extractor
    {
        return $this->extractor;
    }

    public function scan(): FileScan
    {
        return $this->scan;
    }

    public function withPathFilter(Filter $filter): self
    {
        return new self(
            $this->extractor,
            new FileScan(
                $this->scan->limit,
                $this->scan->pathFilter instanceof Filters
                    ? $this->scan->pathFilter->add($filter)
                    : new Filters($this->scan->pathFilter, $filter),
                $this->scan->filter,
            ),
        );
    }

    /**
     * @throws InvalidArgumentException when a limit was already pushed
     */
    public function withFilter(ScalarFunction $filter): self
    {
        return new self($this->extractor, new FileScan($this->scan->limit, $this->scan->pathFilter, match (true) {
            $this->scan->filter === null => $filter,
            $this->scan->filter instanceof All => $this->scan->filter->and($filter),
            default => new All($this->scan->filter, $filter),
        }));
    }

    /**
     * Narrowing only: a second push may lower the limit, never raise it.
     *
     * @throws InvalidArgumentException when $limit is not greater than 0, or a filter was already pushed
     */
    public function withLimit(int $limit): self
    {
        return new self(
            $this->extractor,
            new FileScan(
                $this->scan->limit === null ? $limit : min($this->scan->limit, $limit),
                $this->scan->pathFilter,
                $this->scan->filter,
            ),
        );
    }

    /**
     * @return list<Node>
     */
    public function children(): array
    {
        return [];
    }

    public function withChildren(array $children): self
    {
        return $this;
    }

    /**
     * @throws SchemaNotDerivableException
     */
    public function schema(): Schema
    {
        return $this->extractor->schema();
    }

    public function rowCount(): RowCount
    {
        return RowCount::source;
    }

    public function transparency(): Transparency
    {
        return Transparency::transparent;
    }

    public function materialization(): Materialization
    {
        return Materialization::streaming;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
