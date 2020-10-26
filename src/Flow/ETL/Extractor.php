<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @psalm-immutable
 */
interface Extractor
{
    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract() : \Generator;
}
