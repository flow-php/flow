<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Statistics;

final class DistinctCounter
{
    /**
     * @var array<string, true>
     */
    private array $hashSet;

    public function __construct()
    {
        $this->hashSet = [];
    }

    public function add(string|float|int|\DateTimeInterface|bool $value) : void
    {
        // Normalize value to string for hashing
        if ($value instanceof \DateTimeInterface) {
            $value = $value->getTimestamp();
        }

        $hash = \hash('xxh32', (string) $value);

        if (!\array_key_exists($hash, $this->hashSet)) {
            $this->hashSet[$hash] = true;
        }
    }

    public function count() : int
    {
        return \count($this->hashSet);
    }
}
