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
 * Every left row is paired with every right row; the right side is pulled from the Frame on the first batch.
 */
final readonly class CrossJoin implements Node
{
    public function __construct(
        private Node $input,
        private Frame $frame,
        public string $prefix = '',
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

        return new self($children[0], $frame, $this->prefix);
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
