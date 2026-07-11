<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Rows;
use Generator;

use function array_key_exists;

final class InMemoryCache implements Cache
{
    /**
     * @var array<string, Rows>
     */
    private array $cache = [];

    public function __construct() {}

    public function clear(): void
    {
        $this->cache = [];
    }

    public function delete(string $key): void
    {
        if (!$this->has($key)) {
            return;
        }

        unset($this->cache[$key]);
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
     *
     * @return Generator<int, Rows>
     */
    public function read(string $key): Generator
    {
        yield $this->get($key);
    }

    public function set(string $key, Rows $value): void
    {
        $this->cache[$key] = $value;
    }
}
