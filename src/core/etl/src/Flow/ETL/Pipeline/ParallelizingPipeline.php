<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\chunks_from;
use function Flow\ETL\DSL\from_pipeline;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

/**
 * @deprecated use BatchPipeline instead
 *
 * @internal
 */
final class ParallelizingPipeline implements OverridingPipeline, Pipeline
{
    private readonly Pipeline $nextPipeline;

    /**
     * @param int<1, max> $parallel
     */
    public function __construct(
        private readonly Pipeline $pipeline,
        private readonly int $parallel
    ) {
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

    public function closure(FlowContext $context) : void
    {
        $this->pipeline->closure($context);
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    /**
     * @return array<Pipeline>
     */
    public function pipelines() : array
    {
        $pipelines = [];

        if ($this->pipeline instanceof OverridingPipeline) {
            $pipelines = $this->pipeline->pipelines();
        }
        $pipelines[] = $this->pipeline;

        return $pipelines;
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes()->merge($this->nextPipeline->pipes());
    }

    public function process(FlowContext $context) : \Generator
    {
        $this->nextPipeline->setSource(
            chunks_from(
                from_pipeline($this->pipeline),
                $this->parallel
            )
        );

        return $this->nextPipeline->process($context);
    }

    public function setSource(Extractor $extractor) : self
    {
        $this->pipeline->setSource($extractor);

        return $this;
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
