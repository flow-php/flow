<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Node;

use Flow\ETL\Config\Join\JoinAlgorithmBuilder;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

/**
 * Row count depends on the join type and on both sides' data; the hash join buckets the whole input before emitting.
 */
final readonly class Join implements JoinsFrame
{
    private Result|Outputs $right;

    public function __construct(
        private Node $input,
        Node $right,
        public Expression $on,
        public JoinType $type,
        public ?JoinAlgorithmBuilder $algorithm = null,
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

        return new self($children[0], $children[1], $this->on, $this->type, $this->algorithm);
    }

    public function right(): Result|Outputs
    {
        return $this->right;
    }

    public function rowCount(): RowCount
    {
        return RowCount::unknown;
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
        return Redefined::unknown();
    }
}
