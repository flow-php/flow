<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class NoSimilarityStrategy implements SimilarityStrategy
{
    public function similarity(string $a, string $b) : float
    {
        return 0.0;
    }

    public function threshold() : float
    {
        return 100.0;
    }
}
