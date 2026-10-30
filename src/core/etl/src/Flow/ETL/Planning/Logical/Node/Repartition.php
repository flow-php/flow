<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

/**
 * Buckets the whole input by key before emitting; every row passes, in a different order.
 */
final readonly class Repartition implements Node
{
    public function __construct(
        private Node $input,
        public References $by,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->by);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
