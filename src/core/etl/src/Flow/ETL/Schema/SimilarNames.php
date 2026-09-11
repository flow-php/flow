<?php

declare(strict_types=1);

namespace Flow\ETL\Schema;

use function array_slice;
use function levenshtein;
use function str_contains;
use function strlen;
use function usort;

final readonly class SimilarNames
{
    /**
     * @param list<string> $available
     *
     * @return list<string>
     */
    public function closestTo(string $name, array $available, int $limit = 3): array
    {
        $threshold = strlen($name) / 3;
        $close = [];

        foreach ($available as $candidate) {
            if (levenshtein($name, $candidate) <= $threshold || str_contains($candidate, $name)) {
                $close[] = $candidate;
            }
        }

        usort(
            $close,
            static fn(string $left, string $right): int => levenshtein($name, $left) <=> levenshtein($name, $right),
        );

        return array_slice($close, 0, $limit);
    }
}
