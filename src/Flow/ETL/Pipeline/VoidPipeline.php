<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Extractor;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

final class VoidPipeline implements Pipeline
{
    /**
     * @var Pipeline
     */
    private Pipeline $pipeline;

    public function __construct(Pipeline $pipeline)
    {
        $this->pipeline = $pipeline;
    }

    public function add(Pipe $pipe) : void
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
