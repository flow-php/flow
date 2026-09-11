<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

use function array_key_exists;

final class InMemoryCache implements Cache
{
    /**
     * @var array<string, Rows>
     */
    private array $cache = [];

    /**
     * @var array<string, Schema>
     */
    private array $schemas = [];

    public function __construct() {}

    public function clear(): void
    {
        $this->cache = [];
        $this->schemas = [];
    }

    public function delete(string $key): void
    {
        if (!$this->has($key)) {
            return;
        }

        unset($this->cache[$key], $this->schemas[$key]);
    }

    /**
     * @throws KeyNotInCacheException
     */
    public function get(string $key): Rows
    {
        if (!array_key_exists($key, $this->cache)) {
            throw new KeyNotInCacheException($key);
        }

        return $this->cache[$key];
    }

    public function has(string $key): bool
    {
        return array_key_exists($key, $this->cache);
    }

    /**
     * @throws KeyNotInCacheException
     */
    public function schema(string $key): Schema
    {
        if (!array_key_exists($key, $this->schemas)) {
            throw new KeyNotInCacheException($key);
        }

        return $this->schemas[$key];
    }

    public function set(string $key, Rows $value): void
    {
        $this->cache[$key] = $value;
        $this->schemas[$key] = $value->schema();
    }
}
