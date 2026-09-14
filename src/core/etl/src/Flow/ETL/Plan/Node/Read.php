<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Scan;
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
     * @param Scan $scan what the extractor is handed when the plan runs: the pushed path filter and limit
     */
    public function __construct(
        private Extractor $extractor,
        private Scan $scan = new Scan(),
    ) {}

    public function extractor(): Extractor
    {
        return $this->extractor;
    }

    public function scan(): Scan
    {
        return $this->scan;
    }

    public function withPathFilter(Filter $filter): self
    {
        return new self($this->extractor, $this->scan->withPathFilter($filter));
    }

    public function withLimit(int $limit): self
    {
        return new self($this->extractor, $this->scan->withLimit($limit));
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
