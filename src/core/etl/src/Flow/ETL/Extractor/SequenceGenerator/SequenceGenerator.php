<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\SequenceGenerator;

interface SequenceGenerator
{
    /**
     * @return \Generator<mixed>
     */
    public function generate() : \Generator;
}
