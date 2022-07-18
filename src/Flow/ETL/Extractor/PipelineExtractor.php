<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
final class PipelineExtractor implements Extractor
{
    public function __construct(
        private readonly Pipeline $pipeline
    ) {
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract(FlowContext $context) : \Generator
    {
        /** @psalm-suppress ImpureMethodCall */
        return $this->pipeline->process($context);
    }
}
