<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @psalm-immutable
 */
interface Extractor
{
    /**
     * @return \Generator<Rows>
     */
    public function extract(FlowContext $context) : \Generator;
}
