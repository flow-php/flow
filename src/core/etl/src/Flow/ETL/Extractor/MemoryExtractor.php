<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Memory\Memory;
use Flow\ETL\{Extractor, FlowContext};

final readonly class MemoryExtractor implements Extractor
{
    /**
     * @param Memory $memory
     */
    public function __construct(
        private Memory $memory,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->memory->dump() as $row) {
            $signal = yield array_to_rows([$row], $context->entryFactory());

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
