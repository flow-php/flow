<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline;
use Flow\ETL\Rows;

final class PipelineExtractor implements Extractor
{
    public function __construct(
        private readonly Pipeline $pipeline
    ) {
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<Rows>
     */
    public function extract(FlowContext $context) : \Generator
    {
        return $this->pipeline->process($context);
    }
}
