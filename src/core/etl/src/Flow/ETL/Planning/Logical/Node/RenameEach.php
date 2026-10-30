<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\Transformer\Rename\RenameColumnStrategy;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

final readonly class RenameEach implements Node
{
    /**
     * @param list<RenameColumnStrategy> $strategies
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

    public function redefines(): Redefined
    {
        return Redefined::unknown();
    }
}
