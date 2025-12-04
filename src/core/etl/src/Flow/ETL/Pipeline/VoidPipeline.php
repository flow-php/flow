<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Rows, Transformer};

final readonly class VoidPipeline implements OverridingPipeline, Pipeline
{
    public function __construct(private Pipeline $pipeline)
    {
    }

    #[\Override]
    public function add(Loader|Transformer $pipe) : self
    {
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
        foreach ($this->pipeline->process($context) as $rows) {
            // do nothing, put those rows into void
        }

        yield new Rows();
    }

    #[\Override]
    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
