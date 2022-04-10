<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Extractor;
use Flow\ETL\GroupBy;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Transformer;

final class GroupByPipeline implements Pipeline
{
    private readonly Pipeline $nextPipeline;

    private readonly Pipeline $pipeline;

    public function __construct(private readonly GroupBy $groupBy, Pipeline $pipeline)
    {
        /** @phpstan-ignore-next-line */
        $existingPipeline = $pipeline instanceof self ? $pipeline->pipeline : $pipeline;

        $this->pipeline = $existingPipeline;
        $this->nextPipeline = $existingPipeline->cleanCopy();
    }

    public function add(Loader|Transformer $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function cleanCopy() : Pipeline
    {
        return $this->pipeline->cleanCopy();
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->pipeline->onError($errorHandler);
        $this->nextPipeline->onError($errorHandler);
    }

    public function process(?int $limit = null) : \Generator
    {
        foreach ($this->pipeline->process($limit) as $nextRows) {
            $this->groupBy->group($nextRows);
        }

        $this->nextPipeline->source(new Extractor\ProcessExtractor($this->groupBy->result()));

        foreach ($this->nextPipeline->process() as $nextRows) {
            yield $nextRows;
        }
    }

    public function source(Extractor $extractor) : void
    {
        $this->pipeline->source($extractor);
    }
}
