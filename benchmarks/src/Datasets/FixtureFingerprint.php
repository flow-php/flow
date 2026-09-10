<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use function hash;
use function substr;

final readonly class FixtureFingerprint
{
    public function __construct(
        private FixtureFormat $format,
    ) {}

    /**
     * The filename segment that makes a fixture written by different code a different file.
     */
    public function value(): string
    {
        $buffer = '';

        foreach ($this->format->trees() as $tree) {
            $buffer .= $tree . ':' . DigestCache::tree($tree) . "\n";
        }

        return substr(hash('xxh128', $buffer), 0, 8);
    }
}
