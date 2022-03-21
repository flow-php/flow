<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\ErrorHandler;
use Flow\ETL\Extractor;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

/**
 * @internal
 */
final class CollectingPipeline implements Pipeline
{
    private Pipeline $nextPipeline;

    private Pipeline $pipeline;

    public function __construct(Pipeline $pipeline)
    {
        $this->pipeline = $pipeline;
        $this->nextPipeline = $pipeline->cleanCopy();
    }

    public function add(Pipe $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function cleanCopy() : Pipeline
    {
        return new self($this->pipeline);
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->nextPipeline->onError($errorHandler);
    }

    public function process(?int $limit = null) : \Generator
    {
        $this->nextPipeline->source(From::rows(
            (new Rows())->merge(...\iterator_to_array($this->pipeline->process($limit)))
        ));

        return $this->nextPipeline->process();
    }

    public function source(Extractor $extractor) : void
    {
        $this->pipeline->source($extractor);
    }
}
