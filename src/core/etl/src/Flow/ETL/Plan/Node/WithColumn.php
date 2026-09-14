<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Schema\Definition;

final readonly class WithColumn implements Node
{
    private RowCount $rowCount;

    /**
     * @param Definition<mixed>|string $entry
     */
    public function __construct(
        private Node $input,
        public string|Definition $entry,
        public ScalarFunction $function,
    ) {
        // array_expand() can sit anywhere in the function tree, not only at its root
        $this->rowCount = (new ExpandingFunctions())->in($function) === [] ? RowCount::preserving : RowCount::expanding;
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->entry, $this->function);
    }

    public function rowCount(): RowCount
    {
        return $this->rowCount;
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
        return Redefined::names($this->name());
    }

    public function name(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }
}
