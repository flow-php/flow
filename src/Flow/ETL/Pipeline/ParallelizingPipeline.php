<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\ErrorHandler;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

/**
 * @internal
 */
final class ParallelizingPipeline implements Pipeline
{
    private readonly Pipeline $nextPipeline;

    public function __construct(
        private readonly Pipeline $pipeline,
        private readonly int $parallel
    ) {
        if ($parallel < 1) {
            throw new InvalidArgumentException("Parallel value can't be lower than 1.");
        }

        $this->nextPipeline = $pipeline->cleanCopy();
    }

    public function add(Loader|Transformer $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function cleanCopy() : Pipeline
    {
        return new self($this->pipeline, $this->parallel);
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->nextPipeline->onError($errorHandler);
    }

    public function process(?int $limit = null, callable $callback = null) : \Generator
    {
        $this->nextPipeline->source(
            From::chunks_from(
                From::pipeline($this->pipeline, $limit),
                $this->parallel
            )
        );

        return $this->nextPipeline->process();
    }

    public function source(Extractor $extractor) : void
    {
        $this->pipeline->source($extractor);
    }
}
