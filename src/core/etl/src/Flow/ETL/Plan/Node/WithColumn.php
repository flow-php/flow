<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema\Definition;

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

    public function materialization(): Materialization
    {
        return Materialization::streaming;
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
        return $this->column instanceof Definition ? $this->column->reference()->name() : $this->column;
    }
}
