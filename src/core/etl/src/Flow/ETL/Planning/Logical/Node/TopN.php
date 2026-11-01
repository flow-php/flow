<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

/**
 * Opaque and blocking: which rows survive depends on the whole input.
 */
final readonly class TopN implements Node
{
    /**
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Node $input,
        /** @var list<SortColumn> */
        public array $columns,
        public int $limit,
        public ?SortAlgorithmBuilder $algorithm = null,
    ) {
        if ($this->limit < 1) {
            throw new InvalidArgumentException('TopN limit must be greater than 0, given: ' . $this->limit);
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
        return $children[0] === $this->input
            ? $this
            : new self($children[0], $this->columns, $this->limit, $this->algorithm);
    }

    public function rowCount(): RowCount
    {
        return RowCount::reducing;
    }

    public function transparency(): Transparency
    {
        return Transparency::opaque;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
