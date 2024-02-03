<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use function Flow\ETL\DSL\from_rows;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

final class CachingPipeline implements OverridingPipeline, Pipeline
{
    private readonly Pipeline $nextPipeline;

    public function __construct(private readonly Pipeline $pipeline, private readonly ?string $id = null)
    {
        $this->nextPipeline = $this->pipeline->cleanCopy();
    }

    public function add(Loader|Transformer $pipe) : Pipeline
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

        if ($this->nextPipeline instanceof OverridingPipeline) {
            $pipelines = \array_merge($pipelines, $this->nextPipeline->pipelines());
        }

        $pipelines[] = $this->nextPipeline;

        return $pipelines;
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes()->merge($this->nextPipeline->pipes());
    }

    public function process(FlowContext $context) : \Generator
    {
        $id = $this->id ?: $context->config->id();
        $cacheExists = $context->config->cache()->has($id);

        foreach ($this->pipeline->process($context) as $rows) {
            if (!$cacheExists) {
                $context->config->cache()->add($id, $rows);
            }

            foreach ($this->nextPipeline->setSource(from_rows($rows))->process($context) as $nextRows) {
                yield $nextRows;
            }
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
