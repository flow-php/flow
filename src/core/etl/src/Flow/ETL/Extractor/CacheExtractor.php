<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Cache\CacheIndex, Extractor, FlowContext, Rows};

final class CacheExtractor implements Extractor
{
    private bool $clear = false;

    private ?Extractor $fallbackExtractor = null;

    public function __construct(
        private readonly string $id,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        if (!$context->cache()->has($this->id)) {
            if ($this->fallbackExtractor !== null) {
                foreach ($this->fallbackExtractor->extract($context) as $rows) {
                    $signal = yield $rows;

                    if ($signal === Signal::STOP) {
                        return;
                    }
                }
            }
        } else {
            /** @var CacheIndex $index */
            $index = $context->cache()->get($this->id);

            foreach ($index->values() as $cacheKey) {
                /** @var Rows $rows */
                $rows = $context->cache()->get($cacheKey);
                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    return;
                }

                if ($this->clear) {
                    $context->cache()->delete($cacheKey);
                }
            }
        }

        if ($this->clear && $context->cache()->has($this->id)) {
            $context->cache()->delete($this->id);
        }
    }

    public function withClearOnFinish(bool $clear) : self
    {
        $this->clear = $clear;

        return $this;
    }

    public function withFallbackExtractor(Extractor $fallbackExtractor) : self
    {
        $this->fallbackExtractor = $fallbackExtractor;

        return $this;
    }
}
