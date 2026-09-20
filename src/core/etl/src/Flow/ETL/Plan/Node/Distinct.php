<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Row\Reference;

/**
 * Drops a row whose key was already seen; the decision depends on the row alone and the seen-set is internal to the stream.
 */
final readonly class Distinct implements Node
{
    /**
     * @param list<Reference|string> $entries
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        public array $entries,
    ) {
        if ($this->entries === []) {
            throw new InvalidArgumentException('DropDuplicatesTransformer requires at least one entry');
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->entries);
    }

    public function rowCount(): RowCount
    {
        return RowCount::reducing;
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
