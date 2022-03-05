<?php

declare(strict_types=1);

namespace Flow\ETL\ExternalSort;

use Flow\ETL\Row\Sort;
use Flow\ETL\Rows;

final class CachedParts
{
    /**
     * @var array<string, \Generator<int, Rows, mixed, void>>
     */
    private array $generators;

    /**
     * @param array<string, \Generator<int, Rows, mixed, void>> $generators
     */
    public function __construct(array $generators)
    {
        $this->generators = $generators;
    }

    /**
     * @return array<string>
     */
    public function cacheIds() : array
    {
        return \array_keys($this->generators);
    }

    /**
     * @param Sort ...$entries
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowsMinHeap
     */
    public function createHeap(Sort ...$entries) : RowsMinHeap
    {
        $heap = new RowsMinHeap(...$entries);

        foreach ($this->generators as $cacheId => $generator) {
            if ($generator->valid()) {
                $heap->insert(CachedRow::fromRows($generator->current(), $cacheId));
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
     * @param RowsMinHeap $heap
     * @param string $cacheId
     * @param BufferCache $cache
     *
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
