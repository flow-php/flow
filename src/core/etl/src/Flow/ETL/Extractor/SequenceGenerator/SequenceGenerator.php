<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

interface SequenceGenerator
{
    public function generate() : \Generator;
}
