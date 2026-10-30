<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Path\Filter;

final readonly class Read implements Node
{
    /**
     * @param Extractor $extractor the instance the user handed to read(); the planner never copies it and changes
     *                             nothing but its listing caches, which a run drops when it ends
     *                             (Refreshable::refresh())
     * @param Criteria $criteria what the plan pushed into the read, handed to Extractor::extract() when the plan
     *                           runs; a non-file extractor receives only its limit
     */
    public function __construct(
        private Extractor $extractor,
        private Criteria $criteria = new Criteria(),
    ) {}

    public function extractor(): Extractor
    {
        return $this->extractor;
    }

    public function criteria(): Criteria
    {
        return $this->criteria;
    }

    /**
     * @throws InvalidArgumentException when the extractor is no FileExtractor, which has nowhere to apply it
     */
    public function withFilter(ScalarFunction $filter): self
    {
        return new self($this->fileExtractor(), $this->criteria->withFilter($filter));
    }

    /**
     * Narrowing only: a second push may lower the limit, never raise it.
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function withLimit(int $limit): self
    {
        return new self($this->extractor, $this->criteria->withLimit($limit));
    }

    /**
     * @throws InvalidArgumentException when the extractor is no FileExtractor, which has nowhere to apply it, or
     *                                  when $offset is below 0
     */
    public function withOffset(int $offset): self
    {
        return new self($this->fileExtractor(), $this->criteria->withOffset($offset));
    }

    /**
     * @throws InvalidArgumentException when the extractor is no FileExtractor, which has nowhere to apply it
     */
    public function withPathFilter(Filter $filter): self
    {
        return new self($this->fileExtractor(), $this->criteria->withPathFilter($filter));
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

    public function redefines(): Redefined
    {
        return Redefined::none();
    }

    private function fileExtractor(): FileExtractor
    {
        return $this->extractor instanceof FileExtractor
            ? $this->extractor
            : throw new InvalidArgumentException(
                'Only a FileExtractor reads a pushed filter, ' . $this->extractor::class . ' does not',
            );
    }
}
