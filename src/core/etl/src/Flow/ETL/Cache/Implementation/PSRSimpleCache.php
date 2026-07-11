<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use DateInterval;
use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Rows;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;
use Generator;
use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;

use function is_string;

final readonly class PSRSimpleCache implements Cache
{
    public function __construct(
        private CacheInterface $cache,
        private int|DateInterval|null $ttl = null,
        private Serializer $serializer = new FloeSerializer(),
    ) {}

    public function clear(): void
    {
        $this->cache->clear();
    }

    public function delete(string $key): void
    {
        $this->cache->delete($key);
    }

    public function get(string $key): Rows
    {
        // @mago-ignore analysis:mixed-assignment
        $serializedValue = $this->cache->get($key);

        if (!$serializedValue) {
            throw new KeyNotInCacheException($key);
        }

        try {
            return $this->serializer->unserialize(is_string($serializedValue) ? $serializedValue : '', [Rows::class]);
        } catch (SerializationException $e) {
            throw new KeyNotInCacheException($key, $e);
        }
    }

    public function has(string $key): bool
    {
        try {
            return $this->cache->has($key);
        } catch (InvalidArgumentException) {
            return false;
        }
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
        $this->cache->set($key, $this->serializer->serialize($value), $this->ttl);
    }
}
