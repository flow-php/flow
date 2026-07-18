<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Memory\Memory;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final readonly class MemoryExtractor implements Extractor
{
    /**
     * @param Memory $memory
     */
    public function __construct(
        private Memory $memory,
    ) {}

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->memory->dump() as $row) {
            $signal = yield array_to_rows([$row], $context->hydrator());

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
