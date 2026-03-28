<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class SimilarTextStrategy implements SimilarityStrategy
{
    public function __construct(
        private float $threshold = 50.0,
    ) {
    }

    public function similarity(string $a, string $b) : float
    {
        \similar_text($a, $b, $percent);

        return $percent;
    }

    public function threshold() : float
    {
        return $this->threshold;
    }
}
