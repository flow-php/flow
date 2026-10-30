<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

final readonly class Offset implements Node
{
    /**
     * @param int<0, max> $offset
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        public int $offset,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0, given: ' . $this->offset);
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->offset);
    }

    public function rowCount(): RowCount
    {
        return RowCount::reducing;
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
