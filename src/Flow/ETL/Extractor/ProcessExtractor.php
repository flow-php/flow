<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Extractor;
use Flow\ETL\Rows;

/**
 * @internal
 * @psalm-immutable
 */
final class ProcessExtractor implements Extractor
{
    private Rows $rows;

    public function __construct(Rows $rows)
    {
        $this->rows = $rows;
    }

    public function extract() : \Generator
    {
        yield $this->rows;
    }
}
