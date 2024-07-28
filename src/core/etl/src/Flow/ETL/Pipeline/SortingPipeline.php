<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\OutOfMemoryException;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Row\References;
use Flow\ETL\Sort\{ExternalSort, MemorySort};
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Transformer};

final class SortingPipeline implements Pipeline
{
    public function __construct(private readonly Pipeline $pipeline, private readonly References $refs)
    {
    }

    public function add(Loader|Transformer $pipe) : Pipeline
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
        try {
            if ($context->config->sort->algorithm->useMemory() && $context->config->sort->memoryLimit->isGreaterThan(Unit::fromBytes(0))) {
                $extractor = (new MemorySort($this->pipeline, $context->config->sort->memoryLimit))->sortBy($context, $this->refs);
            } else {
                $extractor = (new ExternalSort($this->pipeline, $context->rowCache()))->sortBy($context, $this->refs);
            }
        } catch (OutOfMemoryException $memoryException) {
            $extractor = (new ExternalSort($this->pipeline, $context->rowCache()))->sortBy($context, $this->refs);
        }

        return $extractor->extract($context);
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
