<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\DataFrameFactory;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

/**
 * The right side is built from each left batch's row values, so no static edge exists and the row count is unknown.
 */
final readonly class JoinEach implements Node
{
    public function __construct(
        private Node $input,
        public DataFrameFactory $factory,
        public Expression $on,
        public JoinType $type,
    ) {}

    /**
     * @return list<Node>
     */
    public function children(): array
    {
        return [$this->input];
    }

    public function withChildren(array $children): self
    {
        return $children[0] === $this->input ? $this : new self($children[0], $this->factory, $this->on, $this->type);
    }

    public function rowCount(): RowCount
    {
        return RowCount::unknown;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function materialization(): Materialization
    {
        return Materialization::streaming;
    }

    public function redefines(): Redefined
    {
        return Redefined::unknown();
    }
}
