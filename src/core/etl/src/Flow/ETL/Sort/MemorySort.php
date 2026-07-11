<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Dataset\Memory\Configuration;
use Flow\ETL\Dataset\Memory\Consumption;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\OutOfMemoryException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

use function max;

final class MemorySort implements SortingAlgorithm
{
    private readonly Configuration $configuration;

    public function __construct(
        private Unit $maximumMemory,
    ) {
        $this->configuration = new Configuration(10);
        $limit = $this->configuration->limit();

        if ($limit !== null && $this->configuration->isLessThan($maximumMemory)) {
            $this->maximumMemory = $limit->percentage(90);
        }
    }

    public function sortGenerator(Generator $rows, FlowContext $context, References $refs): Generator
    {
        $memoryConsumption = new Consumption();
        $maxSize = 1;

        /** @var \Generator<int, Rows> $guardedRows */
        $guardedRows = (function () use ($rows, $memoryConsumption, &$maxSize): Generator {
            foreach ($rows as $batch) {
                $maxSize = max($batch->count(), $maxSize);

                yield $batch;

                if ($memoryConsumption->currentDiff()->isGreaterThan($this->maximumMemory)) {
                    throw new OutOfMemoryException();
                }
            }
        })();

        yield from Rows::mergeAll($guardedRows)
            ->sortBy(...$refs->all())
            ->chunks(max($maxSize, $context->config->cache->externalSortBatchSize));
    }
}
