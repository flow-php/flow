<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class SimilarTextStrategy implements SimilarityStrategy
{
    public function __construct(
        private float $threshold = 50.0,
    ) {}

    public function similarity(string $a, string $b): float
    {
        $totalLength = \strlen($a) + \strlen($b);

        if ($totalLength === 0) {
            return 0.0;
        }

        return ((\similar_text($a, $b) * 2) / $totalLength) * 100;
    }

    public function threshold(): float
    {
        return $this->threshold;
    }
}
