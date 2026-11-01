<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function sprintf;

final class CacheExtractor implements Extractor, Refreshable
{
    private ?Schema $schema = null;

    private bool $clear = false;

    private ?Extractor $fallbackExtractor = null;

    private ?Statistics $statistics = null;

    /**
     * The config's cache, bound by the plan when none was handed to the constructor.
     */
    private ?RowsStorage $bound = null;

    public function __construct(
        private readonly string $id,
        private readonly ?RowsStorage $cache = null,
    ) {}

    /**
     * @return Generator<int, CorruptedRecordException|Rows, Signal|null, void>
     */
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $cache =
            $this->cache
            ?? $this->bound
            ?? throw InvalidLogicException::because(
                'from_cache("%s") was handed no cache and no plan bound the config\'s one: read it through a DataFrame',
                $this->id,
            );
        // a fallback's shape says nothing about a cache hit
        $declared = $this->schema;

        if (!$cache->has($this->id)) {
            if ($this->fallbackExtractor !== null) {
                // a corrupted record passes through: the cache's read has no reject loader, so it is refused
                foreach ($this->fallbackExtractor->extract($backend, new Criteria()) as $rows) {
                    $signal = yield $declared === null || !$rows instanceof Rows
                        ? $rows
                        : $rows->matchTo($declared, $backend);

                    if ($signal === Signal::STOP) {
                        return;
                    }
                }
            }
        } else {
            $folded = $declared ?? ($this->cache === null ? null : $this->schema());

            foreach ($cache->get($this->id) as $cached) {
                $signal = yield $folded === null ? $cached : $cached->matchTo($folded, $backend);

                if ($signal === Signal::STOP) {
                    return;
                }
            }
        }

        if ($this->clear) {
            $cache->remove($this->id);
        }
    }

    /**
     * This source reading $cache when the constructor was handed none; a copy, so the user's source stays unbound.
     */
    public function boundTo(RowsStorage $cache): self
    {
        if ($this->cache !== null) {
            return $this;
        }

        $bound = new self($this->id);
        $bound->schema = $this->schema;
        $bound->clear = $this->clear;
        $bound->fallbackExtractor = $this->fallbackExtractor;
        $bound->bound = $cache;

        return $bound;
    }

    /**
     * A missing entry reads the fallback on every read and never fills the cache, so the fallback must repeat too.
     */
    public function isRepeatable(): bool
    {
        return !$this->clear && ($this->fallbackExtractor?->isRepeatable() ?? true);
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

    public function refresh(): void
    {
        $this->statistics = null;

        if ($this->fallbackExtractor instanceof Refreshable) {
            $this->fallbackExtractor->refresh();
        }
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        // Only a cache handed to the constructor is reachable: extract() may also take one from the
        // FlowContext, which describing a source must not need.
        $cached = $this->cache?->schema($this->id);

        if ($cached !== null) {
            return $cached;
        }

        if ($this->fallbackExtractor !== null) {
            return $this->fallbackExtractor->schema();
        }

        if ($this->cache !== null) {
            throw InferredSchemaException::nothingToInferFrom('from_cache()', sprintf(
                'the cache holds no entry "%s" and no fallback extractor is set',
                $this->id,
            ));
        }

        throw SchemaNotDerivableException::extractor(
            self::class,
            'no cache was handed to from_cache(), and the one in the FlowContext is known only when the pipeline runs',
        );
    }

    /**
     * Answers from the arm extract() will take, and only when a cache was handed to the constructor: one taken from
     * the FlowContext may hold the entry this cache lacks.
     */
    public function statistics(): Statistics
    {
        if ($this->statistics !== null) {
            return $this->statistics;
        }

        if ($this->cache === null) {
            return new Statistics();
        }

        if ($this->cache->has($this->id)) {
            return $this->statistics = new Statistics(rows: $this->cache->rows($this->id));
        }

        if ($this->fallbackExtractor === null) {
            return new Statistics(rows: Cardinality::exact(0));
        }

        return $this->fallbackExtractor->statistics();
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
