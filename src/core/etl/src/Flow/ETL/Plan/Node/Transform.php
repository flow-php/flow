<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Transformer;

/**
 * A user transformer: nothing about its effect on the rows is known, but a Transformer sees one batch at a time so it cannot block.
 * A Stateful one runs as its fresh() instance, so every run starts from the state it was built in.
 */
final readonly class Transform implements Node
{
    public function __construct(
        private Node $input,
        public Transformer $transformer,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->transformer);
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
