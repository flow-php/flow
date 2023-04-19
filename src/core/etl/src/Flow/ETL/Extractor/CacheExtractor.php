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
        private readonly ?Extractor $fallbackExtractor = null,
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
        if (!$this->cache->has($this->id)) {
            if ($this->fallbackExtractor !== null) {
                foreach ($this->fallbackExtractor->extract($context) as $rows) {
                    yield $rows;
                }
            }
        } else {
            foreach ($this->cache->read($this->id) as $rows) {
                yield $rows;
            }

            if ($this->clear) {
                $this->cache->clear($this->id);
            }
        }
    }
}
