<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\from_rows;
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Transformer};

final class CachingPipeline implements Pipeline
{
    public function __construct(private readonly Pipeline $pipeline, private readonly ?string $id = null)
    {
    }

    public function add(Loader|Transformer $pipe) : Pipeline
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
        $id = $this->id ?: $context->config->id();
        $cacheExists = $context->config->cache()->has($id);

        foreach ($this->pipeline->process($context) as $rows) {
            if (!$cacheExists) {
                $context->config->cache()->add($id, $rows);
            }

            yield $rows;
        }
    }

    public function setSource(Extractor $extractor) : Pipeline
    {
        $this->pipeline->setSource($extractor);

        return $this;
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
