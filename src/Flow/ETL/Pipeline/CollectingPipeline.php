<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\DSL\From;
use Flow\ETL\ErrorHandler;
use Flow\ETL\Extractor;
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

    public function add(Loader|Transformer $pipe) : void
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
