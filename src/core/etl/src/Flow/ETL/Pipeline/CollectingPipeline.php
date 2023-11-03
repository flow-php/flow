<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @internal
 */
final class CollectingPipeline implements Pipeline
{
    private readonly Pipeline $nextPipeline;

    public function __construct(private readonly Pipeline $pipeline)
    {
        $this->nextPipeline = $pipeline->cleanCopy();
    }

    public function add(Loader|Transformer $pipe) : self
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

    public function isAsync() : bool
    {
        return $this->pipeline->isAsync();
    }

    public function process(FlowContext $context) : \Generator
    {
        $this->nextPipeline->source(From::rows(
            (new Rows())->merge(...\iterator_to_array($this->pipeline->process($context)))
        ));

        return $this->nextPipeline->process($context);
    }

    public function source(Extractor $extractor) : self
    {
        $this->pipeline->source($extractor);

        return $this;
    }
}
