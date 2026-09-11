<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor\SequenceGenerator\SequenceGenerator;
use Generator;

/**
 * Counts generate() calls so a test can assert how many passes a run makes over the sequence.
 */
final class RecordingSequenceGenerator implements SequenceGenerator
{
    public int $generateCalls = 0;

    public function __construct(
        private readonly SequenceGenerator $inner,
    ) {}

    public function generate(): Generator
    {
        $this->generateCalls++;

        yield from $this->inner->generate();
    }
}
