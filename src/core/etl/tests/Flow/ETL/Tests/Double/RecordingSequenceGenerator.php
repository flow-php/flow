<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\SequenceGenerator\SequenceGenerator;
use Generator;

/**
 * Counts generate() and rows() calls so a test can assert how many passes a run makes over the sequence.
 */
final class RecordingSequenceGenerator implements SequenceGenerator
{
    public int $generateCalls = 0;

    public int $rowsCalls = 0;

    public function __construct(
        private readonly SequenceGenerator $inner,
    ) {}

    public function generate(): Generator
    {
        $this->generateCalls++;

        yield from $this->inner->generate();
    }

    public function rows(): Cardinality
    {
        $this->rowsCalls++;

        return $this->inner->rows();
    }
}
