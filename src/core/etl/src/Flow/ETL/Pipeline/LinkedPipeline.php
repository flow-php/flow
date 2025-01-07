<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Transformer};

/**
 * Purpose of linked pipeline is to keep old pipeline as a source of data and add all elements to the new one that
 * is wrapping the old one.
 *
 * SyncPipeline(OldPipeline)
 *
 * All new elements are added to the SyncPipeline
 */
final readonly class LinkedPipeline implements OverridingPipeline, Pipeline
{
    private Pipeline $nextPipeline;

    public function __construct(
        private Pipeline $pipeline,
    ) {
        $this->nextPipeline = new SynchronousPipeline(new Extractor\PipelineExtractor($this->pipeline));
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->nextPipeline->add($pipe);

        return $this;
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

        if ($this->nextPipeline instanceof OverridingPipeline) {
            $pipelines = \array_merge($pipelines, $this->nextPipeline->pipelines());
        }

        $pipelines[] = $this->nextPipeline;

        return $pipelines;
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes()->merge($this->nextPipeline->pipes());
    }

    public function process(FlowContext $context) : \Generator
    {
        return $this->nextPipeline->process($context);
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
