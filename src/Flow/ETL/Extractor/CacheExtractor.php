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
    public function __construct(
        private readonly string $id,
        private readonly Cache $cache,
        private readonly bool $clear = false
    ) {
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
