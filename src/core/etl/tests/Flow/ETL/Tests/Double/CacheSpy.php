<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Cache;
use Flow\ETL\Rows;

final class CacheSpy implements Cache
{
    private int $clearsCount = 0;

    /**
     * @var array<string, int>
     */
    private array $reads = [];

    private int $readsCount = 0;

    private int $writesCount = 0;

    public function __construct(private readonly Cache $cache)
    {
    }

    public function add(string $id, Rows $rows) : void
    {
        $this->writesCount++;

        $this->cache->add($id, $rows);
    }

    public function clear(string $id) : void
    {
        $this->clearsCount++;

        $this->cache->clear($id);
    }

    public function clears() : int
    {
        return $this->clearsCount;
    }

    public function has(string $id) : bool
    {
        if (!\array_key_exists($id, $this->reads)) {
            return $this->reads[$id] > 0;
        }

        return false;
    }

    public function read(string $id) : \Generator
    {
        if (!\array_key_exists($id, $this->reads)) {
            $this->reads[$id] = 1;
        } else {
            $this->reads[$id]++;
        }

        $this->readsCount++;

        return $this->cache->read($id);
    }

    public function reads() : int
    {
        return $this->readsCount;
    }

    public function writes() : int
    {
        return $this->writesCount;
    }
}
