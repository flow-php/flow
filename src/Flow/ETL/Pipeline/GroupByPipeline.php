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

    private Pipeline $pipeline;

    public function __construct(GroupBy $groupBy, Pipeline $pipeline)
    {
        $this->groupBy = $groupBy;
        $this->pipeline = $pipeline;
    }

    public function add(Pipe $pipe) : void
    {
        $this->pipeline->add($pipe);
    }

    public function clean() : Pipeline
    {
        return new self($this->groupBy, $this->pipeline->clean());
    }

    public function onError(ErrorHandler $errorHandler) : void
    {
        $this->pipeline->onError($errorHandler);
    }

    public function process(?int $limit = null, callable $callback = null) : \Generator
    {
        foreach ($this->pipeline->process($limit) as $nextRows) {
            $this->groupBy->group($nextRows);
        }

        $rows = $this->groupBy->result();

        if ($callback) {
            $callback($rows);
        }

        yield $rows;
    }

    public function source(Extractor $extractor) : void
    {
        $this->pipeline->source($extractor);
    }
}
