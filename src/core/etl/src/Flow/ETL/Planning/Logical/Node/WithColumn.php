<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Values\Schema\Definition;

final readonly class WithColumn implements Node
{
    /**
     * @param Definition<mixed>|string $column
     */
    public function __construct(
        private Node $input,
        public string|Definition $column,
        public ScalarFunction $function,
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
        return Transparency::transparent;
    }

    public function redefines(): Redefined
    {
        // only a bare column reference under a plain name: a Definition column may cast the value
        return is_string($this->column) && $this->function instanceof UnresolvedReference
            ? Redefined::alias($this->name(), $this->function->to())
            : Redefined::names($this->name());
    }

    public function name(): string
    {
        return $this->column instanceof Definition ? $this->column->name() : $this->column;
    }
}
