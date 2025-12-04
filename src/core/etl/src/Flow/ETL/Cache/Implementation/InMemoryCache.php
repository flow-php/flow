<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\{Cache, Row, Rows};
use Flow\ETL\Exception\KeyNotInCacheException;

final class InMemoryCache implements Cache
{
    /**
     * @var array<string, CacheIndex|Row|Rows>
     */
    private array $cache = [];

    public function __construct()
    {
    }

    #[\Override]
    public function clear() : void
    {
        $this->cache = [];
    }

    #[\Override]
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
    #[\Override]
    public function get(string $key) : Row|Rows|CacheIndex
    {
        if (!\array_key_exists($key, $this->cache)) {
            throw new KeyNotInCacheException($key);
        }

        return $this->cache[$key];
    }

    #[\Override]
    public function has(string $key) : bool
    {
        return \array_key_exists($key, $this->cache);
    }

    #[\Override]
    public function set(string $key, CacheIndex|Rows|Row $value) : void
    {
        $this->cache[$key] = $value;
    }
}
