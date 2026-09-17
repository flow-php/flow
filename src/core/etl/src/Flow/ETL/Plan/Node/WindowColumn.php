<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Schema\Definition;

/**
 * A window value is a function of the whole partition, so the input is drained (or repartitioned) before the column is computed.
 */
final readonly class WindowColumn implements Node
{
    /**
     * @param Definition<mixed>|string $entry
     */
    public function __construct(
        private Node $input,
        public string|Definition $entry,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->entry, $this->function);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function materialization(): Materialization
    {
        return Materialization::blocking;
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
