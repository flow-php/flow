<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

/**
 * Drops a row whose key was already seen; the decision depends on the row alone and the seen-set is internal to the stream.
 */
final readonly class Distinct implements Node
{
    /**
     * @param list<Reference|string> $columns
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        public array $columns,
    ) {
        if ($this->columns === []) {
            throw new InvalidArgumentException('Distinct requires at least one column');
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->columns);
    }

    public function rowCount(): RowCount
    {
        return RowCount::reducing;
    }

    public function transparency(): Transparency
    {
        return Transparency::transparent;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
