<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Values\Sort\SortColumn;

use function array_map;

/**
 * Every row passes, but the output order is a function of the whole input, so the input is drained first.
 */
final readonly class Sort implements Node
{
    public function __construct(
        private Node $input,
        /** @var list<SortColumn> */
        public array $columns,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->columns, $this->algorithm);
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

    public function requiredInputColumns(RequiredColumns $output): RequiredColumns
    {
        return $output->with(...array_map(static fn(SortColumn $column): string => $column->name, $this->columns));
    }
}
