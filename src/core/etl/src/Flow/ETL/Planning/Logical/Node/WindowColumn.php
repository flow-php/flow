<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Functions\WindowFunction;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Values\Schema\Definition;

/**
 * A window value is a function of the whole partition, so the input is drained (or repartitioned) before the column is computed.
 */
final readonly class WindowColumn implements Node
{
    /**
     * @param Definition<mixed>|string $column
     */
    public function __construct(
        private Node $input,
        public string|Definition $column,
        public WindowFunction $function,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->column, $this->function);
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
        return Redefined::names($this->name());
    }

    public function name(): string
    {
        return $this->column instanceof Definition ? $this->column->name() : $this->column;
    }
}
