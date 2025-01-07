<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Rows, Transformer};

/**
 * @internal
 */
final readonly class CollectingPipeline implements Pipeline
{
    public function __construct(private Pipeline $pipeline)
    {
    }

    public function add(Loader|Transformer $pipe) : self
    {
        $this->pipeline->add($pipe);

        return $this;
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
        $rows = new Rows();

        foreach ($this->pipeline->process($context) as $nextRows) {
            $rows = $rows->merge($nextRows);
        }

        yield $rows;
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
