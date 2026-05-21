<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Processor\SortingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class SortingProcessorTest extends FlowTestCase
{
    public function test_handles_empty_input(): void
    {
        $processor = new SortingProcessor(refs(ref('id')));

        $configBuilder = config_builder();
        $configBuilder->sort->algorithm(SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT);
        $configBuilder->sortMemoryLimit(Unit::fromMb(10));
        $context = flow_context($configBuilder->build());

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        static::assertCount(0, $result);
    }

    public function test_sorts_across_multiple_batches(): void
    {
        $processor = new SortingProcessor(refs(ref('id')));

        $configBuilder = config_builder();
        $configBuilder->sort->algorithm(SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT);
        $configBuilder->sortMemoryLimit(Unit::fromMb(10));
        $context = flow_context($configBuilder->build());

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)));
            yield rows(row(int_entry('id', 1)));
            yield rows(row(int_entry('id', 2)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, $context));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            $allRows,
        );
    }

    public function test_sorts_rows_ascending(): void
    {
        $processor = new SortingProcessor(refs(ref('id')));

        $configBuilder = config_builder();
        $configBuilder->sort->algorithm(SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT);
        $configBuilder->sortMemoryLimit(Unit::fromMb(10));
        $context = flow_context($configBuilder->build());

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, $context));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            $allRows,
        );
    }

    public function test_sorts_rows_descending(): void
    {
        $processor = new SortingProcessor(refs(ref('id')->desc()));

        $configBuilder = config_builder();
        $configBuilder->sort->algorithm(SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT);
        $configBuilder->sortMemoryLimit(Unit::fromMb(10));
        $context = flow_context($configBuilder->build());

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 3)), row(int_entry('id', 2)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, $context));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(
            [
                ['id' => 3],
                ['id' => 2],
                ['id' => 1],
            ],
            $allRows,
        );
    }
}
