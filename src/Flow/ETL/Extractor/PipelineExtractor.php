<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
final class PipelineExtractor implements Extractor
{
    private ?int $limit;

    /**
     * @var Pipeline
     */
    private Pipeline $pipeline;

    public function __construct(Pipeline $pipeline, ?int $limit = null)
    {
        $this->pipeline = $pipeline;
        $this->limit = $limit;
    }

    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract() : \Generator
    {
        /** @psalm-suppress ImpureMethodCall */
        return $this->pipeline->process($this->limit);
    }
}
