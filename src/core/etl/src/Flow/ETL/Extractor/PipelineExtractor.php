<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Extractor, FlowContext, Pipeline, Rows};

final readonly class PipelineExtractor implements Extractor
{
    public function __construct(
        private Pipeline $pipeline,
    ) {
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<Rows>
     */
    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->pipeline->process($context) as $rows) {
            $signal = yield $rows;

            if ($signal === Signal::STOP) {

                return;
            }
        }
    }
}
