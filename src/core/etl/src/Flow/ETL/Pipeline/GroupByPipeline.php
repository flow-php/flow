<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Extractor, FlowContext, GroupBy, Loader, Pipeline, Transformer};

final readonly class GroupByPipeline implements OverridingPipeline, Pipeline
{
    public function __construct(public GroupBy $groupBy, private Pipeline $pipeline)
    {
    }

    #[\Override]
    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    #[\Override]
    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    #[\Override]
    public function pipelines() : array
    {
        return [$this->pipeline];
    }

    #[\Override]
    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    #[\Override]
    public function process(FlowContext $context) : \Generator
    {
        foreach ($this->pipeline->process($context) as $nextRows) {
            $this->groupBy->group($nextRows, $context);
        }

        yield $this->groupBy->result($context);
    }

    #[\Override]
    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
