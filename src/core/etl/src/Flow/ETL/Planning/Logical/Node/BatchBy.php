<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

final readonly class BatchBy implements Node
{
    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        public Reference $column,
        public ?int $minSize = null,
    ) {
        // @mago-ignore analysis:invalid-operand,impossible-condition,redundant-comparison,redundant-logical-operation
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->column, $this->minSize);
    }

    public function rowCount(): RowCount
    {
        return RowCount::preserving;
    }

    public function transparency(): Transparency
    {
        return Transparency::transparent;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
