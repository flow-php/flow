<?php

declare(strict_types=1);

namespace Flow\ETL;

interface Extractor
{
    /**
     * @return \Generator<Rows>
     */
    public function extract(FlowContext $context) : \Generator;
}
