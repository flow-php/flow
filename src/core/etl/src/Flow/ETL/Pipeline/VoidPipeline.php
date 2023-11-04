<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class VoidPipeline implements Pipeline
{
    public function __construct(private readonly Pipeline $pipeline)
    {
    }

    public function add(Loader|Transformer $pipe) : self
    {
        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return new self($this->pipeline->cleanCopy());
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
        foreach ($this->pipeline->process($context) as $rows) {
            // do nothing, put those rows into void
        }

        yield new Rows();
    }

    public function setSource(Extractor $extractor) : self
    {
        return $this;
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }
}
