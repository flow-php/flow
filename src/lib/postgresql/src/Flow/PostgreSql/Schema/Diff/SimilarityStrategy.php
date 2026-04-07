<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

interface SimilarityStrategy
{
    /**
     * Returns a similarity score between two names as a percentage (0.0–100.0).
     */
    public function similarity(string $a, string $b) : float;

    /**
     * Minimum similarity score (0.0–100.0) required to consider two names as a rename candidate.
     */
    public function threshold() : float;
}
