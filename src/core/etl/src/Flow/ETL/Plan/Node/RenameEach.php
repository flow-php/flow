<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Transformer\Rename\RenameEntryStrategy;

final readonly class RenameEach implements Node
{
    /**
     * @param list<RenameEntryStrategy> $strategies
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        public array $strategies,
    ) {
        if ($this->strategies === []) {
            throw new InvalidArgumentException('At least one strategy must be provided.');
        }
    }

    /**
     * @return list<Node>
     */
    public function children(): array
    {
        return [$this->input];
    }

    public function withChildren(array $children): self
    {
        return $children[0] === $this->input ? $this : new self($children[0], $this->strategies);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
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
        return Redefined::unknown();
    }
}
