<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cache;
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

final class CacheExtractor implements Extractor
{
    private bool $clear = false;

    private ?Extractor $fallbackExtractor = null;

    public function __construct(
        private readonly string $id,
        private readonly ?Cache $cache = null,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $cache = $this->cache ?? $context->cache();

        if (!$cache->has($this->id)) {
            if ($this->fallbackExtractor !== null) {
                foreach ($this->fallbackExtractor->extract($context) as $rows) {
                    $signal = yield $rows;

                    if ($signal === Signal::STOP) {
                        return;
                    }
                }
            }
        } else {
            $index = CacheIndex::fromRows($this->id, $cache->get($this->id));

            foreach ($index->values() as $cacheKey) {
                $signal = yield $cache->get($cacheKey);

                if ($signal === Signal::STOP) {
                    return;
                }

                if ($this->clear) {
                    $cache->delete($cacheKey);
                }
            }
        }

        if ($this->clear && $cache->has($this->id)) {
            $cache->delete($this->id);
        }
    }

    public function withClearOnFinish(bool $clear): self
    {
        $this->clear = $clear;

        return $this;
    }

    public function withFallbackExtractor(Extractor $fallbackExtractor): self
    {
        $this->fallbackExtractor = $fallbackExtractor;

        return $this;
    }
}
