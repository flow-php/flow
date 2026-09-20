<?php

declare(strict_types=1);

namespace Flow\ETL\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Loader;
use Flow\ETL\Sink;
use Flow\ETL\Transformation;

final readonly class Branched implements Sink
{
    public function __construct(
        private ScalarFunction $condition,
        private Loader|Sink $sink,
    ) {}

    public function write(DataFrame $prefix): void
    {
        $prefix->filter($this->condition)->write($this->sink);
    }

    /**
     * BC shim for to_branch($condition, $loader, $transformation) - must precede write().
     */
    public function withTransformation(Transformation $transformation): self
    {
        return new self($this->condition, new Transformed($transformation, $this->sink));
    }
}
