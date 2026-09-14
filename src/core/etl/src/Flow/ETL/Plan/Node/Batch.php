<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

final readonly class Batch implements Node
{
    /**
     * @param int<1, max> $size
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        public int $size,
    ) {
        // @mago-ignore analysis:invalid-operand,impossible-condition,redundant-comparison
        if ($this->size <= 0) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->size);
        }
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->size);
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
        return Redefined::none();
    }
}
