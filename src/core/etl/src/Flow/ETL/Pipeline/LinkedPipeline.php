<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Extractor\PipelineExtractor;
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
        $this->nextPipeline = new SynchronousPipeline(new PipelineExtractor($this->pipeline));
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
        return [$this->pipeline, $this->nextPipeline];
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
