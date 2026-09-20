<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;

/**
 * A leaf that is not a Read. No DataFrame verb builds one, so it is the only way to reach the code
 * paths that guard a row-input chain which does not end in a source.
 */
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

    public function materialization(): Materialization
    {
        return Materialization::streaming;
    }

    public function redefines(): Redefined
    {
        return Redefined::none();
    }
}
