<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Rows;

/**
 * @implements Cache<array<mixed>>
 */
final class InMemoryCache implements Cache
{
    /**
     * @var array<string, array<Rows>>
     */
    private array $cache = [];

    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function add(string $id, Rows $rows) : void
    {
        if (!\array_key_exists($id, $this->cache)) {
            $this->cache[$id] = [];
        }

        $this->cache[$id][] = $rows;
    }

    public function clear(string $id) : void
    {
        if (\array_key_exists($id, $this->cache)) {
            $this->cache[$id] = [];
        }
    }

    public function has(string $id) : bool
    {
        return \array_key_exists($id, $this->cache);
    }

    public function read(string $id) : \Generator
    {
        if (\array_key_exists($id, $this->cache)) {
            foreach ($this->cache[$id] as $rows) {
                yield $rows;
            }
        }
    }
}
