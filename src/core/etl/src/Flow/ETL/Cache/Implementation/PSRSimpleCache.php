<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use Flow\ETL\Cache\{CacheIndex};
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\{Cache, Row, Rows};
use Flow\Serializer\{NativePHPSerializer, Serializer};
use Psr\SimpleCache\{CacheInterface, InvalidArgumentException};

final readonly class PSRSimpleCache implements Cache
{
    public function __construct(
        private CacheInterface $cache,
        private int|\DateInterval|null $ttl = null,
        private Serializer $serializer = new NativePHPSerializer(),
    ) {
    }

    public function clear() : void
    {
        $this->cache->clear();
    }

    public function delete(string $key) : void
    {
        $this->cache->delete($key);
    }

    public function get(string $key) : Row|Rows|CacheIndex
    {
        $serializedValue = $this->cache->get($key);

        if (!$serializedValue) {
            throw new KeyNotInCacheException($key);
        }

        return $this->serializer->unserialize($serializedValue, [Row::class, Rows::class, CacheIndex::class]);
    }

    public function has(string $key) : bool
    {
        try {
            return $this->cache->has($key);
        } catch (InvalidArgumentException) {
            return false;
        }
    }

    public function set(string $key, CacheIndex|Rows|Row $value) : void
    {
        $this->cache->set($key, $this->serializer->serialize($value), $this->ttl);
    }
}
