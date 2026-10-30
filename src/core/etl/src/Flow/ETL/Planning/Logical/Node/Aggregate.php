<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

/**
 * Every group becomes one row, and the rows of a group can arrive in any batch, so the input is drained before the first row is emitted.
 */
final readonly class Aggregate implements Node
{
    public function __construct(
        private Node $input,
        public GroupBy $groupBy,
        public ?BucketingAlgorithmBuilder $algorithm = null,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->groupBy, $this->algorithm);
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
        return Redefined::unknown();
    }
}
