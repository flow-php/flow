<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\SequenceGenerator;

use Flow\ETL\Values\Cardinality;
use Generator;

interface SequenceGenerator
{
    /**
     * @return \Generator<mixed>
     */
    public function generate(): Generator;

    public function rows(): Cardinality;
}
