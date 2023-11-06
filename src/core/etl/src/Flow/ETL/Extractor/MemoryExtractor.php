<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\Memory;

final class MemoryExtractor implements Extractor
{
    /**
     * @param Memory $memory
     */
    public function __construct(
        private readonly Memory $memory,
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
