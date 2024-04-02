<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

final class CachedParts
{
    /**
     * @param array<string, \Generator<Rows>> $generators
     */
    public function __construct(private readonly array $generators)
    {
    }

    /**
     * @return array<string>
     */
    public function cacheIds() : array
    {
        return \array_keys($this->generators);
    }

    /**
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     */
    public function createHeap(Reference ...$refs) : RowsMinHeap
    {
        $heap = new RowsMinHeap(...$refs);

        foreach ($this->generators as $cacheId => $generator) {
            if ($generator->valid()) {
                /** @var Rows $rows */
                $rows = $generator->current();

                foreach ($rows->chunks(1) as $chunk) {
                    $heap->insert(CachedRow::fromRows($chunk, $cacheId));
                }
                $generator->next();
            }
        }

        return $heap;
    }

    public function notEmpty() : bool
    {
        foreach ($this->generators as $generator) {
            if ($generator->valid()) {
                return true;
            }
        }

        return false;
    }

    /**
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     */
    public function takeNext(RowsMinHeap $heap, string $cacheId, BufferCache $cache) : void
    {
        $minRow = $heap->extract();

        $cache->add($cacheId, $minRow->toRows());

        if ($this->generators[$minRow->cacheId()]->valid()) {
            $heap->insert(CachedRow::fromRows($this->generators[$minRow->cacheId()]->current(), $minRow->cacheId()));
            $this->generators[$minRow->cacheId()]->next();
        } else {
            foreach ($this->generators as $partialCacheId => $generator) {
                if ($generator->valid()) {
                    $heap->insert(CachedRow::fromRows($generator->current(), $partialCacheId));
                    $generator->next();

                    break;
                }
            }
        }
    }
}
