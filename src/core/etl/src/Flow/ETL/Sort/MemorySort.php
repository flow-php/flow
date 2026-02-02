<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\{Dataset\Memory\Configuration,
    Dataset\Memory\Consumption,
    Dataset\Memory\Unit,
    Exception\OutOfMemoryException,
    FlowContext,
    Row\References,
    Rows};

final class MemorySort implements SortingAlgorithm
{
    private readonly Configuration $configuration;

    public function __construct(
        private Unit $maximumMemory,
    ) {
        $this->configuration = new Configuration(10);

        if ($this->configuration->isLessThan($maximumMemory) && !$this->configuration->isInfinite()) {
            /**
             * @phpstan-ignore-next-line
             */
            $this->maximumMemory = $this->configuration->limit()->percentage(90);
        }
    }

    public function sortGenerator(\Generator $rows, FlowContext $context, References $refs) : \Generator
    {
        $memoryConsumption = new Consumption();
        $mergedRows = new Rows();
        $maxSize = 1;

        foreach ($rows as $batch) {
            $maxSize = \max($batch->count(), $maxSize);
            $mergedRows = $mergedRows->merge($batch);

            if ($memoryConsumption->currentDiff()->isGreaterThan($this->maximumMemory)) {
                throw new OutOfMemoryException();
            }
        }

        yield from $mergedRows->sortBy(...$refs->all())->chunks($maxSize);
    }
}
