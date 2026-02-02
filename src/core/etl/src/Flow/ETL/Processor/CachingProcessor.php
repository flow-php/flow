<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\{FlowContext, Processor};

/**
 * Caches pipeline output for reuse.
 *
 * If a cache with the given id already exists, data passes through unchanged.
 * Otherwise, each batch is cached before being yielded.
 *
 * @internal
 */
final readonly class CachingProcessor implements Processor
{
    public function __construct(private ?string $id = null)
    {
    }

    public function process(\Generator $rows, FlowContext $context) : \Generator
    {
        $id = $this->id ?: $context->config->id();
        $cacheIndexExists = $context->cache()->has($id);

        if ($cacheIndexExists) {
            yield from $rows;

            return;
        }

        $index = new CacheIndex($id);

        foreach ($rows as $batch) {
            $cacheKey = \bin2hex(\random_bytes(16));
            $context->cache()->set($cacheKey, $batch);
            $index->add($cacheKey);

            yield $batch;
        }

        $context->cache()->set($id, $index);
    }
}
