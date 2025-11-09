<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{Extractor, FlowContext, Loader, Pipeline, Row, Rows, Transformer};
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Schema\Definition;

final readonly class WindowFunctionPipeline implements OverridingPipeline, Pipeline
{
    /**
     * @param Definition<mixed>|string $entry
     */
    public function __construct(
        private Pipeline $pipeline,
        private string|Definition $entry,
        private WindowFunction $function,
    ) {
    }

    public function add(Loader|Transformer $pipe) : Pipeline
    {
        $this->pipeline->add($pipe);

        return $this;
    }

    public function has(string $transformerClass) : bool
    {
        return $this->pipeline->has($transformerClass);
    }

    public function pipelines() : array
    {
        return [$this->pipeline];
    }

    public function pipes() : Pipes
    {
        return $this->pipeline->pipes();
    }

    /**
     * @return \Generator<int, Rows>
     */
    public function process(FlowContext $context) : \Generator
    {
        $currentPartitionKey = null;
        $partitionRows = [];

        foreach ($this->pipeline->process($context) as $rows) {
            foreach ($rows as $row) {
                $partitionKey = $this->extractPartitionKey($row);

                if ($currentPartitionKey !== null && $currentPartitionKey !== $partitionKey) {
                    $processedRows = $this->processPartition($partitionRows, $context);

                    if ($processedRows->count() > 0) {
                        yield $processedRows;
                    }

                    $partitionRows = [];
                }

                $partitionRows[] = $row;
                $currentPartitionKey = $partitionKey;
            }
        }

        if ([] !== $partitionRows) {
            $processedRows = $this->processPartition($partitionRows, $context);

            if ($processedRows->count() > 0) {
                yield $processedRows;
            }
        }
    }

    public function source() : Extractor
    {
        return $this->pipeline->source();
    }

    private function extractPartitionKey(Row $row) : string
    {
        $partitions = $this->function->window()->partitions();

        if ([] === $partitions) {
            return '__single_partition__';
        }

        $keyParts = [];

        foreach ($partitions as $partition) {
            try {
                $keyParts[] = $row->valueOf($partition);
            } catch (InvalidArgumentException) {
                $keyParts[] = null;
            }
        }

        return \serialize($keyParts);
    }

    /**
     * @param array<Row> $rows
     */
    private function processPartition(array $rows, FlowContext $context) : Rows
    {
        if ([] === $rows) {
            return new Rows();
        }

        $partitionRows = new Rows(...$rows);

        $orderBy = $this->function->window()->order();

        if ([] !== $orderBy) {
            $partitionRows = $partitionRows->sortBy(...$orderBy);
        }

        $processedRows = [];

        foreach ($partitionRows as $row) {
            $value = $this->function->apply($row, $partitionRows, $context);

            $entryName = $this->entry instanceof Definition
                ? $this->entry->entry()->name()
                : $this->entry;

            $newRow = $row->add(
                $context->entryFactory()->create(
                    $entryName,
                    $value,
                    $this->entry instanceof Definition ? $this->entry : null
                )
            );

            $processedRows[] = $newRow;
        }

        return new Rows(...$processedRows);
    }
}
