<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\DataFrame;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Generator;

final readonly class DataFrameExtractor implements Extractor
{
    public function __construct(
        private DataFrame $dataFrame,
    ) {}

    /**
     * @return Generator<int, \Flow\ETL\Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        foreach ($this->dataFrame->get() as $rows) {
            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
