<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

final class CacheIndex
{
    /**
     * @var array<string>
     */
    private array $index = [];

    public function __construct(public readonly string $key)
    {
    }

    public function add(string $value) : void
    {
        $this->index[] = $value;
    }

    /**
     * @return array<string>
     */
    public function values() : array
    {
        return $this->index;
    }
}
