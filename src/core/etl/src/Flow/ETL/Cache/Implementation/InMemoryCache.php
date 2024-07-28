<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache\{Cache, CacheIndex};
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\{Row, Rows};

final class InMemoryCache implements Cache
{
    /**
     * @var array<string, CacheIndex|Row|Rows>
     */
    private array $cache = [];

    public function __construct()
    {
    }

    public function clear() : void
    {
        $this->cache = [];
    }

    public function delete(string $key) : void
    {
        if (!$this->has($key)) {
            return;
        }

        unset($this->cache[$key]);
    }

    /**
     * @throws KeyNotInCacheException
     */
    public function get(string $key) : Row|Rows|CacheIndex
    {
        if (!\array_key_exists($key, $this->cache)) {
            throw new KeyNotInCacheException($key);
        }

        return $this->cache[$key];
    }

    public function has(string $key) : bool
    {
        return \array_key_exists($key, $this->cache);
    }

    public function set(string $key, CacheIndex|Rows|Row $value) : void
    {
        $this->cache[$key] = $value;
    }
}
