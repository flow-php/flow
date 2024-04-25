<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Extractor, FlowContext, GroupBy, Loader, Pipeline, Transformer};

final class GroupByPipeline implements Pipeline
{
    private readonly Pipeline $pipeline;

    public function __construct(public readonly GroupBy $groupBy, Pipeline $pipeline)
    {
        $this->pipeline = $pipeline;
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipeline->add($pipe);

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

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    public function process(FlowContext $context) : \Generator
    {
        foreach ($this->pipeline->process($context) as $nextRows) {
            $this->groupBy->group($nextRows);
        }

        yield $this->groupBy->result($context);
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
