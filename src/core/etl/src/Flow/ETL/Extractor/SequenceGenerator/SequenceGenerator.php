<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

use Generator;

interface SequenceGenerator
{
    /**
     * @return \Generator<mixed>
     */
    public function generate(): Generator;
}
