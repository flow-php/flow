<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;
use Flow\ETL\Values\Join\JoinType;

/**
 * The right side is built from each left batch's row values, so no static edge exists and the row count is unknown.
 */
final readonly class JoinEach implements Node
{
    public function __construct(
        private Node $input,
        public ExtractorFactory $factory,
        public Expression $on,
        public JoinType $type,
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
        return $children[0] === $this->input ? $this : new self($children[0], $this->factory, $this->on, $this->type);
    }

    public function rowCount(): RowCount
    {
        return RowCount::unknown;
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
