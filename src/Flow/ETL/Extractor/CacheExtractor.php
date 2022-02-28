<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cache;
use Flow\ETL\Extractor;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
final class CacheExtractor implements Extractor
{
    private Cache $cache;

    private bool $clear;

    private string $id;

    public function __construct(string $id, Cache $cache, bool $clear = false)
    {
        $this->cache = $cache;
        $this->id = $id;
        $this->clear = $clear;
    }

    /**
     * @psalm-suppress ImpureMethodCall
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract() : \Generator
    {
        foreach ($this->cache->read($this->id) as $rows) {
            yield $rows;
        }

        if ($this->clear) {
            $this->cache->clear($this->id);
        }
    }
}
