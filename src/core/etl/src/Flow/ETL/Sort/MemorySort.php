<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\{Exception\OutOfMemoryException,
    Extractor,
    FlowContext,
    Monitoring\Memory\Configuration,
    Monitoring\Memory\Consumption,
    Monitoring\Memory\Unit,
    Pipeline,
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

    public function sortBy(Pipeline $pipeline, FlowContext $context, References $refs) : Extractor
    {
        $memoryConsumption = new Consumption();
        $mergedRows = new Rows();
        $maxSize = 1;

        foreach ($pipeline->process($context) as $rows) {
            $maxSize = \max($rows->count(), $maxSize);
            $mergedRows = $mergedRows->merge($rows);

            if ($memoryConsumption->currentDiff()->isGreaterThan($this->maximumMemory)) {
                throw new OutOfMemoryException();
            }
        }

        return new Extractor\GeneratorExtractor($mergedRows->sortBy(...$refs->all())->chunks($maxSize));
    }
}
