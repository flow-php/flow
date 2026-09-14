<?php

declare(strict_types=1);

namespace Flow\ETL\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Loader;
use Flow\ETL\Sink;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;

/**
 * @import-type Sinks from \Flow\ETL\Plan\LogicalPlan
 */
final readonly class Transformed implements Sink
{
    public function __construct(
        private Transformer|Transformation $transformer,
        private Loader|Sink $sink,
    ) {}

    /**
     * @return Sinks
     */
    public function roots(DataFrame $prefix): array
    {
        return (new Roots())->of($prefix->with($this->transformer), $this->sink);
    }
}
