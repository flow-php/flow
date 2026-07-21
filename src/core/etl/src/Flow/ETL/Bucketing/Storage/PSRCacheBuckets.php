<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing\Storage;

use DateInterval;
use Flow\ETL\Bucketing\BucketsStorage;
use Flow\ETL\Rows;
use Flow\Floe\FloeSerializer;
use Flow\Serializer\Serializer;
use Generator;
use Psr\SimpleCache\CacheInterface;

use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
use function is_int;
use function is_string;

final readonly class PSRCacheBuckets implements BucketsStorage
{
    public function __construct(
        private CacheInterface $cache,
        private string $prefix = 'flow:buckets',
        private ?DateInterval $ttl = null,
        private Serializer $serializer = new FloeSerializer(),
    ) {}

    public function append(string $bucketId, Rows $rows): void
    {
        $n = $this->chunkCount($bucketId);

        $this->cache->set($this->chunkKey($bucketId, $n), serialize_to_string($this->serializer, $rows), $this->ttl);
        $this->cache->set($this->chunksKey($bucketId), $n + 1, $this->ttl);
    }

    /**
     * @return Generator<Rows>
     */
    public function get(string $bucketId): Generator
    {
        $chunks = $this->chunkCount($bucketId);

        for ($i = 0; $i < $chunks; $i++) {
            // @mago-ignore analysis:mixed-assignment
            $payload = $this->cache->get($this->chunkKey($bucketId, $i));

            if (!is_string($payload)) {
                continue;
            }

            yield unserialize_from_string($this->serializer, $payload);
        }
    }

    public function remove(string $bucketId): void
    {
        $chunks = $this->chunkCount($bucketId);

        $keys = [$this->chunksKey($bucketId)];

        for ($i = 0; $i < $chunks; $i++) {
            $keys[] = $this->chunkKey($bucketId, $i);
        }

        $this->cache->deleteMultiple($keys);
    }

    public function set(string $bucketId, Rows $rows): void
    {
        $this->remove($bucketId);
        $this->append($bucketId, $rows);
    }

    private function chunkCount(string $bucketId): int
    {
        // @mago-ignore analysis:mixed-assignment
        $count = $this->cache->get($this->chunksKey($bucketId), 0);

        return is_int($count) ? $count : 0;
    }

    private function chunkKey(string $bucketId, int $n): string
    {
        return $this->prefix . ':' . $bucketId . ':chunk:' . $n;
    }

    private function chunksKey(string $bucketId): string
    {
        return $this->prefix . ':' . $bucketId . ':chunks';
    }
}
