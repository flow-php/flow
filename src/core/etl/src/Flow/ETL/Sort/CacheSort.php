<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Sort\ExternalSort\{CachedRow, RowsMinHeap};

final class CacheSort
{
    /**
     * @param array<string, \Generator<Row>> $generators
     */
    public function __construct(private readonly array $generators)
    {
    }

    /**
     * @return \Generator<Row>
     */
    public function sort(Reference ...$refs) : \Generator
    {
        $heap = new RowsMinHeap(...$refs);

        $generatorsCopy = $this->generators;

        foreach ($generatorsCopy as $generatorId => $generator) {
            if ($generator->valid()) {
                $row = new CachedRow($generator->current(), $generatorId);
                $heap->insert($row);
                $generator->next();
            } else {
                unset($generatorsCopy[$generatorId]);
            }
        }

        while (!$heap->isEmpty()) {
            /** @var CachedRow $cachedRow */
            $cachedRow = $heap->extract();

            yield $cachedRow->row;

            if (isset($generatorsCopy[$cachedRow->generatorId])) {
                $generator = $generatorsCopy[$cachedRow->generatorId];

                if ($generator->valid()) {
                    $row = new CachedRow($generator->current(), $cachedRow->generatorId);
                    $heap->insert($row);
                    $generator->next();
                } else {
                    unset($generatorsCopy[$cachedRow->generatorId]);  // Remove the empty generator
                }
            }
        }
    }
}
