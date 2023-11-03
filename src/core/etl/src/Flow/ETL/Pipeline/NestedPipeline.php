<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

final class NestedPipeline implements Pipeline
{
    public function __construct(
        private readonly Pipeline $currentPipeline,
        private readonly Pipeline $nextPipeline
    ) {
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->nextPipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return new self(
            $this->currentPipeline->cleanCopy(),
            $this->nextPipeline->cleanCopy(),
        );
    }

    public function closure(FlowContext $context) : void
    {
        $this->currentPipeline->closure($context);
    }

    public function has(string $transformerClass) : bool
    {
        return $this->currentPipeline->has($transformerClass);
    }

    public function isAsync() : bool
    {
        return $this->currentPipeline->isAsync() || $this->nextPipeline->isAsync();
    }

    public function process(FlowContext $context) : \Generator
    {
        foreach ($this->nextPipeline->source(new Extractor\PipelineExtractor($this->currentPipeline))->process($context) as $rows) {
            yield $rows;
        }
    }

    public function source(Extractor $extractor) : Pipeline
    {
        $this->currentPipeline->source($extractor);

        return $this;
    }
}
