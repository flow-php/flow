<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

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

    public function isAsync() : bool
    {
        return false;
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    public function process(FlowContext $context) : \Generator
    {
        $context->config->cache()->clear($id = $this->id ?: $context->config->id());

        foreach ($this->pipeline->process($context) as $rows) {
            $context->config->cache()->add($id, $rows);
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
