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
    public function __construct(
        private readonly Pipeline $pipeline,
        private readonly ?int $limit = null
    ) {
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
