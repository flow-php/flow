<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Bucketing;

use function array_key_exists;
use function array_unique;
use function count;
use function hash;

/**
 * The keys a stream brought so far, each held as its 16-byte xxh128 digest, so a long key costs no more than a short
 * one.
 */
final class SeenKeys
{
    /**
     * @var array<string, true>
     */
    private array $seen = [];

    /**
     * @param list<string> $keys one per row
     *
     * @return list<int> the rows whose key was never seen before, the first row of each, ascending
     */
    public function add(array $keys): array
    {
        $new = [];

        foreach (array_unique($keys) as $row => $key) {
            $digest = hash('xxh128', $key, true);

            if (!array_key_exists($digest, $this->seen)) {
                $this->seen[$digest] = true;
                $new[] = $row;
            }
        }

        return $new;
    }

    public function count(): int
    {
        return count($this->seen);
    }
}
