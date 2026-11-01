<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Window;

use Flow\Benchmarks\Partitioning\PartitionCardinality;
use Flow\ETL\API\DataFrame;
use Flow\ETL\Functions\Reference\Reference;

/**
 * Both arms derive the key so the pair differs by partition key alone.
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
