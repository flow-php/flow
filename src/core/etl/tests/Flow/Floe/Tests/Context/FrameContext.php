<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Context;

use function array_values;
use function pack;
use function substr_replace;
use function unpack;

final class FrameContext
{
    /**
     * @return list<int> the row, node and buffer counts, then every node and extent word
     */
    public static function directory(string $body): array
    {
        /** @var array{1: int, 2: int, 3: int} $counts */
        $counts = unpack('V3', $body);

        /** @var list<int> */
        return array_values((array) unpack('V' . (3 + (2 * ($counts[2] + $counts[3]))), $body));
    }

    /**
     * @param array<int, int> $words directory word index (0 = row count) => new value
     */
    public static function patch(string $body, array $words): string
    {
        foreach ($words as $index => $word) {
            $body = substr_replace($body, pack('V', $word), 4 * $index, 4);
        }

        return $body;
    }
}
