<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\{Cache\RowsCache, Rows};

final class CacheSpy implements RowsCache
{
    private int $clearsCount = 0;

    /**
     * @var array<string, int>
     */
    private array $reads = [];

    private int $readsCount = 0;

    private int $writesCount = 0;

    public function __construct(private readonly RowsCache $cache)
    {
    }

    public function append(string $key, Rows $rows) : void
    {
        $this->writesCount++;

        $this->cache->append($key, $rows);
    }

    public function clears() : int
    {
        return $this->clearsCount;
    }

    public function get(string $key) : \Generator
    {
        if (!\array_key_exists($key, $this->reads)) {
            $this->reads[$key] = 1;
        } else {
            $this->reads[$key]++;
        }

        $this->readsCount++;

        return $this->cache->get($key);
    }

    public function has(string $key) : bool
    {
        if (\array_key_exists($key, $this->reads)) {
            return $this->reads[$key] > 0;
        }

        return false;
    }

    public function reads() : int
    {
        return $this->readsCount;
    }

    public function remove(string $key) : void
    {
        $this->clearsCount++;

        $this->cache->remove($key);
    }

    public function writes() : int
    {
        return $this->writesCount;
    }
}
