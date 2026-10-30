<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Functions\WithColumn;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

use function array_map;

final readonly class DuplicateRow implements Node
{
    /**
     * @param list<WithColumn> $columns
     */
    public function __construct(
        private Node $input,
        public mixed $condition,
        public array $columns,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->condition, $this->columns);
    }

    public function rowCount(): RowCount
    {
        return RowCount::expanding;
    }

    public function transparency(): Transparency
    {
        return Transparency::transparent;
    }

    public function redefines(): Redefined
    {
        return Redefined::names(...array_map(static fn(WithColumn $column): string => $column->name, $this->columns));
    }
}
