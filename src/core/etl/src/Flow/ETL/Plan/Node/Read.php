<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\File\FileExtractor;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Schema;
use Flow\Filesystem\Path\Filter;

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

    /**
     * @throws InvalidArgumentException when the extractor is no FileExtractor, which has nowhere to apply it
     */
    public function withFilter(ScalarFunction $filter): self
    {
        return new self($this->fileExtractor(), $this->scan->withFilter($filter));
    }

    /**
     * Narrowing only: a second push may lower the limit, never raise it.
     *
     * @throws InvalidArgumentException when $limit is not greater than 0
     */
    public function withLimit(int $limit): self
    {
        return new self($this->extractor, $this->scan->withLimit($limit));
    }

    /**
     * @throws InvalidArgumentException when the extractor is no FileExtractor, which has nowhere to apply it
     */
    public function withPathFilter(Filter $filter): self
    {
        return new self($this->fileExtractor(), $this->scan->withPathFilter($filter));
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

    private function fileExtractor(): FileExtractor
    {
        return $this->extractor instanceof FileExtractor
            ? $this->extractor
            : throw new InvalidArgumentException(
                'Only a FileExtractor reads a pushed filter, ' . $this->extractor::class . ' does not',
            );
    }
}
