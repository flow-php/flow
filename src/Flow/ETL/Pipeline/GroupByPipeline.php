<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Config;
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

    public function add(Loader|Transformer $pipe) : self
    {
        $this->nextPipeline->add($pipe);

        return $this;
    }

    public function cleanCopy() : Pipeline
    {
        return $this->pipeline->cleanCopy();
    }

    public function process(Config $config) : \Generator
    {
        foreach ($this->pipeline->process($config) as $nextRows) {
            $this->groupBy->group($nextRows);
        }

        $this->nextPipeline->source(new Extractor\ProcessExtractor($this->groupBy->result()));

        foreach ($this->nextPipeline->process($config) as $nextRows) {
            yield $nextRows;
        }
    }

    public function source(Extractor $extractor) : self
    {
        $this->pipeline->source($extractor);

        return $this;
    }
}
