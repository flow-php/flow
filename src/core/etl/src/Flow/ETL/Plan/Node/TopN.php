<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Row\References;

/**
 * The first $limit rows of the input sorted by $refs - a Sort under a Limit, holding only $limit rows. Which rows
 * survive depends on the whole input, so it is opaque and blocking.
 */
final readonly class TopN implements Node
{
    /**
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        public References $refs,
        public int $limit,
    ) {
        if ($this->limit < 1) {
            throw new InvalidArgumentException('TopN limit must be greater than 0, given: ' . $this->limit);
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->refs, $this->limit);
    }

    public function rowCount(): RowCount
    {
        return RowCount::reducing;
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
