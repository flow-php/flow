<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Extractor;
use Flow\ETL\GroupBy;
use Flow\ETL\Pipeline;

final class GroupByPipeline implements Pipeline
{
    private GroupBy $groupBy;

    private Pipeline $nextPipeline;

    private Pipeline $pipeline;

    public function __construct(GroupBy $groupBy, Pipeline $pipeline)
    {
        $existingPipeline = $pipeline instanceof self ? $pipeline->pipeline : $pipeline;

        $this->groupBy = $groupBy;
        $this->pipeline = $existingPipeline;
        $this->nextPipeline = $existingPipeline->clean();
    }

    public function add(Pipe $pipe) : void
    {
        $this->nextPipeline->add($pipe);
    }

    public function clean() : Pipeline
    {
        return $this->pipeline->clean();
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->pipeline->onError($errorHandler);
        $this->nextPipeline->onError($errorHandler);
    }

    public function process(?int $limit = null, callable $callback = null) : \Generator
    {
        foreach ($this->pipeline->process($limit) as $nextRows) {
            $this->groupBy->group($nextRows);
        }

        $this->nextPipeline->source(new Extractor\ProcessExtractor($this->groupBy->result()));

        foreach ($this->nextPipeline->process(null, $callback) as $nextRows) {
            yield $nextRows;
        }
    }

    public function source(Extractor $extractor) : void
    {
        $this->pipeline->source($extractor);
    }
}
