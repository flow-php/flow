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
final readonly class Join implements Node
{
    public function __construct(
        private Node $input,
        private Frame $frame,
        public Expression $on,
        public JoinType $type,
        public ?JoinAlgorithmBuilder $algorithm = null,
    ) {}

    /**
     * @return list<Node>
     */
    public function children(): array
    {
        return [$this->input, $this->frame];
    }

    public function withChildren(array $children): self
    {
        if ($children[0] === $this->input && $children[1] === $this->frame) {
            return $this;
        }

        $frame = $children[1];

        if (!$frame instanceof Frame) {
            throw InvalidLogicException::because(
                'The side input of %s is always a Frame, %s given',
                self::class,
                $frame::class,
            );
        }

        return new self($children[0], $frame, $this->on, $this->type, $this->algorithm);
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
