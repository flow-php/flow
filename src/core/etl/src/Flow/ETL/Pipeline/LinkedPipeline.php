<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Transformer};

final class LinkedPipeline implements OverridingPipeline, Pipeline
{
    private readonly Pipeline $nextPipeline;

    public function __construct(
        private readonly Pipeline $pipeline,
    ) {
        $this->nextPipeline = new SynchronousPipeline(new Extractor\PipelineExtractor($this->pipeline));
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->nextPipeline->add($pipe);

        return $this;
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
        foreach ($this->nextPipeline->process($context) as $rows) {
            yield $rows;
        }
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
