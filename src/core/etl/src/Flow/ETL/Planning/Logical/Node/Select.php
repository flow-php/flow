<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

use function array_map;

final readonly class Select implements Node
{
    /**
     * @param list<Reference|string> $columns
     */
    public function __construct(
        private Node $input,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->columns);
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
        return Redefined::allBut(...References::init(...$this->columns)->names());
    }

    public function requiredInputColumns(RequiredColumns $output): RequiredColumns
    {
        return RequiredColumns::only(...array_map(
            static fn(Reference $column): string => $column->base(),
            References::init(...$this->columns)->all(),
        ));
    }
}
