<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
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

    public function add(Loader|Transformer $pipe) : self
    {
        $this->nextPipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return new self($this->pipeline, $this->parallel);
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    public function isAsync() : bool
    {
        return $this->pipeline->isAsync();
    }

    public function process(FlowContext $context) : \Generator
    {
        $this->nextPipeline->source(
            From::chunks_from(
                From::pipeline($this->pipeline),
                $this->parallel
            )
        );

        return $this->nextPipeline->process($context);
    }

    public function source(Extractor $extractor) : self
    {
        $this->pipeline->source($extractor);

        return $this;
    }
}
