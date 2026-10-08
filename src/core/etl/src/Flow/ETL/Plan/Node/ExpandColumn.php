<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RequiredColumns;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Schema\Definition;

final readonly class ExpandColumn implements Node
{
    public RequiredColumns $carries;

    /**
     * @param Definition<mixed>|string $entry
     * @param null|RequiredColumns $carries what the consumers above read; null means every column
     */
    public function __construct(
        private Node $input,
        public string|Definition $entry,
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
            : new self($children[0], $this->entry, $this->function, $this->carries);
    }

    public function withCarries(RequiredColumns $carries): self
    {
        return new self($this->input, $this->entry, $this->function, $carries);
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
        return Redefined::names($this->name());
    }

    public function name(): string
    {
        return $this->entry instanceof Definition ? $this->entry->entry()->name() : $this->entry;
    }
}
