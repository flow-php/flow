<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\ErrorHandler;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Pipeline;

/**
 * @internal
 */
final class ParallelizingPipeline implements Pipeline
{
    private Pipeline $nextPipeline;

    private int $parallel;

    private Pipeline $pipeline;

    public function __construct(Pipeline $pipeline, int $parallel)
    {
        if ($parallel < 1) {
            throw new InvalidArgumentException("Parallel value can't be lower than 1.");
        }

        $this->pipeline = $pipeline;
        $this->parallel = $parallel;
        $this->nextPipeline = $pipeline->clean();
    }

    public function add(Pipe $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function clean() : Pipeline
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

        return $this->nextPipeline->process($limit, $callback);
    }

    public function source(Extractor $extractor) : void
    {
        $this->pipeline->source($extractor);
    }
}
