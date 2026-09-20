<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Config\Sort\SortAlgorithmBuilder;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Row\References;

/**
 * Every row passes, but the output order is a function of the whole input, so the input is drained first.
 */
final readonly class Sort implements Node
{
    public function __construct(
        private Node $input,
        public References $refs,
        public ?SortAlgorithmBuilder $algorithm = null,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->refs, $this->algorithm);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function materialization(): Materialization
    {
        return Materialization::blocking;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
