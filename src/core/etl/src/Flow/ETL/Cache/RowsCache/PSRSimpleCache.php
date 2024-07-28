<?php

declare(strict_types=1);

namespace Flow\ETL\Cache\RowsCache;

use Flow\ETL\{Cache\RowsCache, Rows};
use Flow\Serializer\{NativePHPSerializer, Serializer};
use Psr\SimpleCache\CacheInterface;

final class PSRSimpleCache implements RowsCache
{
    public function __construct(
        private readonly CacheInterface $cache,
        private readonly int|\DateInterval|null $ttl = null,
        private readonly Serializer $serializer = new NativePHPSerializer()
    ) {
    }

    public function append(string $key, Rows $rows) : void
    {
        $rowsId = $rows->hash();

        $this->addToIndex($key, $rowsId);
        $this->cache->set($rowsId, $this->serializer->serialize($rows), $this->ttl);
    }

    public function get(string $key) : \Generator
    {
        foreach ($this->index($key) as $entry) {
            $serializedRows = $this->cache->get($entry);

            if ($serializedRows === null) {
                continue;
            }

            /**
             * @var Rows $rows
             */
            $rows = $this->serializer->unserialize((string) $serializedRows, Rows::class);

            yield $rows;
        }
    }

    public function has(string $key) : bool
    {
        return \count($this->index($key)) > 0;
    }

    public function remove(string $key) : void
    {
        foreach ($this->index($key) as $entry) {
            $this->cache->delete($entry);
        }
    }

    private function addToIndex(string $indexId, string $id) : void
    {
        /** @var null|array<string> $index */
        $index = $this->cache->get($indexId);
        $this->cache->set($indexId, \array_merge($index ?? [], [$id]), $this->ttl);
    }

    /**
     * @param string $indexId
     *
     * @return array<string>
     */
    private function index(string $indexId) : array
    {
        /** @var null|array<string> $index */
        $index = $this->cache->get($indexId);

        return $index ?? [];
    }
}
