<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Cache;
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function bin2hex;
use function random_bytes;

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
    public function __construct(
        private ?string $id = null,
        private ?Cache $cache = null,
    ) {}

    /**
     * @param Generator<int, Rows> $rows
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $id = $this->id ?: $context->config->id();
        $cache = $this->cache ?? $context->cache();
        $cacheIndexExists = $cache->has($id);

        if ($cacheIndexExists) {
            yield from $rows;

            return;
        }

        $index = new CacheIndex($id);
        $stopped = false;

        // a stop still reads the rest: an index over a prefix would serve truncated data on the next read
        foreach ($rows as $batch) {
            $cacheKey = bin2hex(random_bytes(16));
            $cache->set($cacheKey, $batch);
            $index->add($cacheKey);

            if (!$stopped) {
                $signal = yield $batch;
                $stopped = $signal === Signal::STOP;
            }
        }

        $cache->set($id, $index->toRows());
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }
}
