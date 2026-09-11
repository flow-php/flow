<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor\SequenceGenerator\SequenceGenerator;
use Generator;

final class MixedSequenceGenerator implements SequenceGenerator
{
    public function generate(): Generator
    {
        yield 1000;
        yield 'AB-01';
    }
}
