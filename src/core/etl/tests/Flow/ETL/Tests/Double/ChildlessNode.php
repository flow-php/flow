<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Redefined;
use Flow\ETL\Planning\Logical\RowCount;
use Flow\ETL\Planning\Logical\Transparency;

final readonly class ChildlessNode implements Node
{
    public function children(): array
    {
        return [];
    }

    public function withChildren(array $children): Node
    {
        return $this;
    }

    public function rowCount(): RowCount
    {
        return RowCount::source;
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
