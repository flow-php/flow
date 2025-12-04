<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\{Cache\CacheIndex, Extractor, FlowContext, Loader, Pipeline, Transformer};

final readonly class CachingPipeline implements OverridingPipeline, Pipeline
{
    public function __construct(private Pipeline $pipeline, private ?string $id = null)
    {
    }

    #[\Override]
    public function add(Loader|Transformer $pipe) : Pipeline
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
        $id = $this->id ?: $context->config->id();
        $cacheIndexExists = $context->cache()->has($id);

        if ($cacheIndexExists) {
            foreach ($this->pipeline->process($context) as $rows) {
                yield $rows;
            }

            return;
        }

        $index = new CacheIndex($id);

        foreach ($this->pipeline->process($context) as $rows) {
            $cacheKey = bin2hex(random_bytes(16));
            $context->cache()->set($cacheKey, $rows);
            $index->add($cacheKey);

            yield $rows;
        }

        $context->cache()->set($id, $index);
    }

    #[\Override]
    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
