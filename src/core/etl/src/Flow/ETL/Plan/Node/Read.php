<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Schema;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function min;

final readonly class Read implements Node
{
    /**
     * @param Extractor $extractor the instance the user handed to read(); the planner never mutates or copies it
     * @param null|int<1, max> $limit the pushed limit, handed to Extractor::extract() when the plan runs
     * @param Filter $pathFilter the pushed partition filter, handed to FileExtractor::extract() when the plan runs
     */
    public function __construct(
        private Extractor $extractor,
        private ?int $limit = null,
        private Filter $pathFilter = new OnlyFiles(),
    ) {}

    public function extractor(): Extractor
    {
        return $this->extractor;
    }

    /**
     * @return null|int<1, max>
     */
    public function limit(): ?int
    {
        return $this->limit;
    }

    public function pathFilter(): Filter
    {
        return $this->pathFilter;
    }

    public function withPathFilter(Filter $filter): self
    {
        return new self(
            $this->extractor,
            $this->limit,
            $this->pathFilter instanceof Filters
                ? $this->pathFilter->add($filter)
                : new Filters($this->pathFilter, $filter),
        );
    }

    /**
     * Narrowing only: a second push may lower the limit, never raise it.
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function withLimit(int $limit): self
    {
        if ($limit <= 0) {
            throw new InvalidArgumentException('Limit must be greater than 0');
        }

        return new self(
            $this->extractor,
            $this->limit === null ? $limit : min($this->limit, $limit),
            $this->pathFilter,
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
