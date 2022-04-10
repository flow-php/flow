<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class VoidPipeline implements Pipeline
{
    public function __construct(private readonly Pipeline $pipeline)
    {
    }

    public function add(Loader|Transformer $pipe) : void
    {
    }

    public function cleanCopy() : Pipeline
    {
        return new self($this->pipeline->cleanCopy());
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
    }

    /**
     * @psalm-suppress UnusedForeachValue
     */
    public function process(?int $limit = null) : \Generator
    {
        foreach ($this->pipeline->process($limit) as $rows) {
            // do nothing, put those rows into void
        }

        yield new Rows();
    }

    public function source(Extractor $extractor) : void
    {
    }
}
