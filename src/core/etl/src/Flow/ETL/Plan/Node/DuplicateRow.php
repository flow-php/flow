<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\WithEntry;

use function array_map;

final readonly class DuplicateRow implements Node
{
    /**
     * @param list<WithEntry> $entries
     */
    public function __construct(
        private Node $input,
        public mixed $condition,
        public array $entries,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->condition, $this->entries);
    }

    public function rowCount(): RowCount
    {
        return RowCount::expanding;
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
        return Redefined::names(...array_map(static fn(WithEntry $entry): string => $entry->name, $this->entries));
    }
}
