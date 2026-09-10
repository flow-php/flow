<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Window;

use Flow\Benchmarks\Partitioning\PartitionCardinality;
use Flow\ETL\DataFrame;
use Flow\ETL\Row\Reference;

/**
 * Both arms derive the key even though only the high-cardinality one partitions by it, so the pair
 * differs by partition key alone. Deriving it only for `high` would fold a per-row temporal transform
 * into the delta the axis exists to measure.
 */
final readonly class WindowPartitioning
{
    public function __construct(
        private PartitionCardinality $cardinality,
    ) {}

    public function derive(DataFrame $frame): DataFrame
    {
        return $this->cardinality->derive($frame);
    }

    public function reference(): Reference
    {
        return $this->cardinality->reference();
    }
}
