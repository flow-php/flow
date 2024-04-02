<?php

declare(strict_types=1);

namespace Flow\ETL\Cache;

use Flow\ETL\{Cache, Rows};
use Flow\Serializer\{NativePHPSerializer, Serializer};
use Psr\SimpleCache\CacheInterface;

final class PSRSimpleCache implements Cache
{
    public function __construct(
        private readonly CacheInterface $cache,
        private readonly int|\DateInterval|null $ttl = null,
        private readonly Serializer $serializer = new NativePHPSerializer()
    ) {
    }

    public function add(string $id, Rows $rows) : void
    {
        $rowsId = $rows->hash();

        $this->addToIndex($id, $rowsId);
        $this->cache->set($rowsId, $this->serializer->serialize($rows), $this->ttl);
    }

    public function clear(string $id) : void
    {
        foreach ($this->index($id) as $entry) {
            $this->cache->delete($entry);
        }
    }

    public function has(string $id) : bool
    {
        return \count($this->index($id)) > 0;
    }

    public function read(string $id) : \Generator
    {
        foreach ($this->index($id) as $entry) {
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
