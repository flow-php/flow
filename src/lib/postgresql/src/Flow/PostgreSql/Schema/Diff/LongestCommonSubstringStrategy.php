<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

final readonly class LongestCommonSubstringStrategy implements SimilarityStrategy
{
    public function __construct(
        private float $threshold = 50.0,
    ) {
    }

    public function similarity(string $a, string $b) : float
    {
        $maxLen = \max(\strlen($a), \strlen($b));

        if ($maxLen === 0) {
            return 100.0;
        }

        $lenA = \strlen($a);
        $lenB = \strlen($b);
        $longest = 0;

        $prev = \array_fill(0, $lenB + 1, 0);

        for ($i = 1; $i <= $lenA; $i++) {
            $curr = \array_fill(0, $lenB + 1, 0);

            for ($j = 1; $j <= $lenB; $j++) {
                if ($a[$i - 1] === $b[$j - 1]) {
                    $curr[$j] = $prev[$j - 1] + 1;

                    if ($curr[$j] > $longest) {
                        $longest = $curr[$j];
                    }
                }
            }

            $prev = $curr;
        }

        return ($longest / $maxLen) * 100.0;
    }

    public function threshold() : float
    {
        return $this->threshold;
    }
}
