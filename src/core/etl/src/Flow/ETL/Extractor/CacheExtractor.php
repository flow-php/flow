<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cache;
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;

final class CacheExtractor implements Extractor
{
    private ?Schema $schema = null;

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
                    if ($this->schema !== null) {
                        $rows = array_to_rows($rows->toArray(), $context->hydrator(), $this->schema);
                    }

                    $signal = yield $rows;

                    if ($signal === Signal::STOP) {
                        return;
                    }
                }
            }
        } else {
            $index = CacheIndex::fromRows($this->id, $cache->get($this->id));

            foreach ($index->values() as $cacheKey) {
                $rows = $cache->get($cacheKey);

                if ($this->schema !== null) {
                    $rows = array_to_rows($rows->toArray(), $context->hydrator(), $this->schema);
                }

                $signal = yield $rows;

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

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        // Only a cache handed to the constructor is reachable: extract() may also take one from the
        // FlowContext, which describing a source must not need.
        if ($this->cache !== null && $this->cache->has($this->id)) {
            $schema = new Schema();

            foreach (CacheIndex::fromRows($this->id, $this->cache->get($this->id))->values() as $cacheKey) {
                $schema = $schema->merge($this->cache->schema($cacheKey));
            }

            return $schema;
        }

        if ($this->fallbackExtractor !== null) {
            return $this->fallbackExtractor->schema();
        }

        // A cache entry that does not exist yet has no columns. That is an answer, not an
        // unanswerable question, and threading a cache in from the FlowContext to look harder
        // would break the rule the comment above states.
        return new Schema();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
