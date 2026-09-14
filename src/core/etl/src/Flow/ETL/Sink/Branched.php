<?php

declare(strict_types=1);

namespace Flow\ETL\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Loader;
use Flow\ETL\Sink;
use Flow\ETL\Transformation;

/**
 * @import-type Sinks from \Flow\ETL\Plan\LogicalPlan
 */
final readonly class Branched implements Sink
{
    public function __construct(
        private ScalarFunction $condition,
        private Loader|Sink $sink,
    ) {}

    /**
     * @return Sinks
     */
    public function roots(DataFrame $prefix): array
    {
        return (new Roots())->of($prefix->filter($this->condition), $this->sink);
    }

    /**
     * BC shim for to_branch($condition, $loader, $transformation) - must precede write().
     */
    public function withTransformation(Transformation $transformation): self
    {
        return new self($this->condition, new Transformed($transformation, $this->sink));
    }
}
