<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Values\Schema\Definition;

final readonly class ExpandColumn implements Node
{
    public RequiredColumns $carries;

    /**
     * @param Definition<mixed>|string $column
     * @param null|RequiredColumns $carries what the consumers above read; null means every column
     */
    public function __construct(
        private Node $input,
        public string|Definition $column,
        public ScalarFunction $function,
        ?RequiredColumns $carries = null,
    ) {
        $this->carries = $carries ?? RequiredColumns::all();
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
        return $children[0] === $this->input
            ? $this
            : new self($children[0], $this->column, $this->function, $this->carries);
    }

    public function withCarries(RequiredColumns $carries): self
    {
        return new self($this->input, $this->column, $this->function, $carries);
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
        return Redefined::names($this->name());
    }

    public function name(): string
    {
        return $this->column instanceof Definition ? $this->column->name() : $this->column;
    }
}
