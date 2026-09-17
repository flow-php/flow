<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

/**
 * Every left row is paired with every right row; the right side is pulled on the first batch.
 */
final readonly class CrossJoin implements JoinsFrame
{
    private Result|Outputs $right;

    public function __construct(
        private Node $input,
        Node $right,
        public string $prefix = '',
    ) {
        $this->right =
            $right instanceof Result || $right instanceof Outputs
                ? $right
                : throw InvalidLogicException::joinSideIsNotAPlanRoot($right::class);
    }

    /**
     * @return list<Node>
     */
    public function children(): array
    {
        return [$this->input, $this->right];
    }

    public function withChildren(array $children): self
    {
        if ($children[0] === $this->input && $children[1] === $this->right) {
            return $this;
        }

        return new self($children[0], $children[1], $this->prefix);
    }

    public function right(): Result|Outputs
    {
        return $this->right;
    }

    public function rowCount(): RowCount
    {
        return RowCount::expanding;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function materialization(): Materialization
    {
        return Materialization::streaming;
    }

    public function redefines(): Redefined
    {
        return Redefined::unknown();
    }
}
