<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use function array_key_exists;
use function array_keys;

final class HashTable
{
    /**
     * @var array<int, list<int>>
     */
    private array $buckets = [];

    private int $count = 0;

    /**
     * @var array<int, true>
     */
    private array $unmatched = [];

    public function __construct(
        private readonly bool $trackUnmatched = false,
    ) {}

    public function add(int $group, int $index): void
    {
        $this->buckets[$group][] = $index;
        $this->count++;

        if ($this->trackUnmatched) {
            $this->unmatched[$index] = true;
        }
    }

    /**
     * @return list<int> the build-side rows of $group, in the order they were added
     */
    public function candidatesFor(int $group): array
    {
        return array_key_exists($group, $this->buckets) ? $this->buckets[$group] : [];
    }

    public function count(): int
    {
        return $this->count;
    }

    public function matched(int $index): void
    {
        unset($this->unmatched[$index]);
    }

    /**
     * @return list<int> the build-side rows never matched, in the order they were added
     */
    public function unmatched(): array
    {
        return array_keys($this->unmatched);
    }
}
