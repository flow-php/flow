<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CachingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\ConstrainedProcessor;
use Flow\ETL\Operators\Processor\CountingProcessor;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Processor\ExpandingProcessor;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\MergeSortProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Operators\Processor\RequiredColumns;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Operators\Processor\VoidProcessor;
use Flow\ETL\Operators\Processor\WindowProcessor;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Tests\Mother\GroupByAggregationProcessorMother;
use Flow\ETL\Tests\Mother\RepartitionProcessorMother;
use Flow\ETL\Values\BatchSize;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\window;

final class ProcessorBreaksTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{Processor, bool}>
     */
    public static function processors(): Generator
    {
        $groupBy = new GroupBy(refs('id'), new Aggregators(count(ref('id'))));

        yield 'group by' => [GroupByAggregationProcessorMother::inMemory($groupBy), true];
        yield 'pivot' => [new PivotProcessor($groupBy, config()->grouping), true];
        yield 'memory sort' => [new MemorySortProcessor([ref('id')->asc()]), true];
        yield 'merge sort' => [
            new MergeSortProcessor(
                ExternalSortMother::inMemoryConfig(),
                [ref('id')->asc()],
                new NativePHPRandomValueGenerator(),
            ),
            true,
        ];
        yield 'top n' => [
            new TopNProcessor([ref('id')->asc()], 1, new MemorySortConfig(), new NativePHPRandomValueGenerator()),
            true,
        ];
        yield 'repartition' => [RepartitionProcessorMother::inMemory(refs(ref('id'))), true];
        yield 'counting' => [new CountingProcessor(), true];
        yield 'collecting' => [new CollectingProcessor(), true];
        yield 'void' => [new VoidProcessor(), true];
        yield 'window' => [new WindowProcessor('rn', row_number()->over(window()->partitionBy(ref('id')))), false];
        yield 'batching' => [new BatchingProcessor(new BatchSize(2)), false];
        yield 'batching by' => [new BatchingByProcessor(ref('id')), false];
        yield 'constrained' => [new ConstrainedProcessor(), false];
        yield 'drop duplicates' => [new DistinctProcessor(['id']), false];
        yield 'offset' => [new OffsetProcessor(1), false];
        yield 'expanding' => [new ExpandingProcessor('item', ref('items')->expand(), RequiredColumns::all()), false];
        yield 'caching' => [new CachingProcessor('id', new MemoryRowsStorage()), false];
    }

    #[DataProvider('processors')]
    public function test_a_processor_breaks_only_when_it_needs_every_row_before_it_emits(
        Processor $processor,
        bool $breaks,
    ): void {
        static::assertSame($breaks, $processor->breaks());
    }
}
