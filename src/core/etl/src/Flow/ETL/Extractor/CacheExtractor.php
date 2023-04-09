<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cache;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;

final class CacheExtractor implements Extractor
{
    public function __construct(
        private readonly string $id,
        private readonly Cache $cache,
        private readonly bool $clear = false
    ) {
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->cache->read($this->id) as $rows) {
            yield $rows;
        }

        if ($this->clear) {
            $this->cache->clear($this->id);
        }
    }
}
