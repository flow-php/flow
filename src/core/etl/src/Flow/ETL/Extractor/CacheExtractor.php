<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\{Cache\CacheIndex, Extractor, FlowContext, Rows};

final class CacheExtractor implements Extractor
{
    public function __construct(
        private readonly string $id,
        private readonly ?Extractor $fallbackExtractor = null,
        private readonly bool $clear = false
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
}
