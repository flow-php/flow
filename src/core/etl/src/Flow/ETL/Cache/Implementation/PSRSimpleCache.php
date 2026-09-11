<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\Implementation;

use DateInterval;
use Flow\ETL\Cache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Exception\SerializationException;
use Flow\Serializer\Serializer;
use JsonException;
use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;

use function Flow\ETL\DSL\schema_from_json;
use function Flow\ETL\DSL\schema_to_json;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
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
        $this->cache->delete($this->schemaKey($key));
    }

    public function get(string $key): Rows
    {
        // @mago-ignore analysis:mixed-assignment
        $serializedValue = $this->cache->get($key);

        if (!$serializedValue) {
            throw new KeyNotInCacheException($key);
        }

        try {
            return unserialize_from_string($this->serializer, is_string($serializedValue) ? $serializedValue : '');
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

    public function schema(string $key): Schema
    {
        // @mago-ignore analysis:mixed-assignment
        $serializedSchema = $this->cache->get($this->schemaKey($key));

        if (!is_string($serializedSchema) || $serializedSchema === '') {
            throw new KeyNotInCacheException($key);
        }

        try {
            return schema_from_json($serializedSchema);
        } catch (JsonException $e) {
            throw new KeyNotInCacheException($key, $e);
        }
    }

    public function set(string $key, Rows $value): void
    {
        $this->cache->set($key, serialize_to_string($this->serializer, $value), $this->ttl);
        $this->cache->set($this->schemaKey($key), schema_to_json($value->schema()), $this->ttl);
    }

    private function schemaKey(string $key): string
    {
        return $key . '.schema';
    }
}
