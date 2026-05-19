<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use function levenshtein;
use function max;
use function strlen;

final readonly class LevenshteinStrategy implements SimilarityStrategy
{
    public function __construct(
        private float $threshold = 50.0,
    ) {}

    public function similarity(string $a, string $b): float
    {
        $maxLen = max(strlen($a), strlen($b));

        if ($maxLen === 0) {
            return 100.0;
        }

        return (1.0 - (levenshtein($a, $b) / $maxLen)) * 100.0;
    }

    public function threshold(): float
    {
        return $this->threshold;
    }
}
