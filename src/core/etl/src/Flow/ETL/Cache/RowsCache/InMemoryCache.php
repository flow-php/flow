<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\RowsCache;

use Flow\ETL\{Cache\RowsCache, Rows};

final class InMemoryCache implements RowsCache
{
    /**
     * @var array<string, array<Rows>>
     */
    private array $cache = [];

    public function append(string $key, Rows $rows) : void
    {
        if (!\array_key_exists($key, $this->cache)) {
            $this->cache[$key] = [];
        }

        $this->cache[$key][] = $rows;
    }

    public function get(string $key) : \Generator
    {
        if (\array_key_exists($key, $this->cache)) {
            foreach ($this->cache[$key] as $rows) {
                yield $rows;
            }
        }
    }

    public function has(string $key) : bool
    {
        return \array_key_exists($key, $this->cache);
    }

    public function remove(string $key) : void
    {
        if (\array_key_exists($key, $this->cache)) {
            $this->cache[$key] = [];
        }
    }
}
