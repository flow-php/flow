<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache\{Cache, CacheIndex};
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\{Row, Rows};
use Flow\Serializer\{NativePHPSerializer, Serializer};
use Psr\SimpleCache\{CacheInterface, InvalidArgumentException};

final class PSRSimpleCache implements Cache
{
    public function __construct(
        private readonly CacheInterface $cache,
        private readonly int|\DateInterval|null $ttl = null,
        private readonly Serializer $serializer = new NativePHPSerializer()
    ) {
    }

    public function clear() : void
    {
        $this->cache->clear();
    }

    public function delete(string $key) : void
    {
        try {
            $this->cache->delete($key);
        } catch (InvalidArgumentException $e) {
            throw new KeyNotInCacheException("Key {$key} not found in cache");
        }
    }

    public function get(string $key) : Row|Rows|CacheIndex
    {
        try {
            $serializedValue = $this->cache->get($key);
        } catch (InvalidArgumentException $e) {
            throw new KeyNotInCacheException("Key {$key} not found in cache");
        }

        return $this->serializer->unserialize((string) $serializedValue, [Row::class, Rows::class, CacheIndex::class]);
    }

    public function has(string $key) : bool
    {
        try {
            return $this->cache->has($key);
        } catch (InvalidArgumentException $e) {
            return false;
        }
    }

    public function set(string $key, CacheIndex|Rows|Row $value) : void
    {
        $this->cache->set($key, $this->serializer->serialize($value), $this->ttl);
    }
}
