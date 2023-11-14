<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

final class GroupByPipeline implements OverridingPipeline, Pipeline
{
    private readonly Pipeline $nextPipeline;

    private readonly Pipeline $pipeline;

    public function __construct(private readonly GroupBy $groupBy, Pipeline $pipeline)
    {
        $existingPipeline = $pipeline instanceof self ? $pipeline->pipeline : $pipeline;

        $this->pipeline = $existingPipeline;
        $this->nextPipeline = $existingPipeline->cleanCopy();
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->nextPipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return $this->pipeline->cleanCopy();
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
        foreach ($this->pipeline->process($context) as $nextRows) {
            $this->groupBy->group($nextRows);
        }

        $this->nextPipeline->setSource(new Extractor\ProcessExtractor($this->groupBy->result($context)));

        foreach ($this->nextPipeline->process($context) as $nextRows) {
            yield $nextRows;
        }
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
