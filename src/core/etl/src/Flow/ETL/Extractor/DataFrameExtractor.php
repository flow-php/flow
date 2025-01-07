<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{DataFrame, Extractor, FlowContext};

final readonly class DataFrameExtractor implements Extractor
{
    public function __construct(private DataFrame $dataFrame)
    {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->dataFrame->get() as $rows) {
            $signal = yield $rows;

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
