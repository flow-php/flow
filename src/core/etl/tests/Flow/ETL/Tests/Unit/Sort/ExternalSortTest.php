<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\Double\ThrowingRemoveBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Tests\Mother\SortDatasetMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_filter;
use function array_map;
use function array_merge;
use function array_sum;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;
use function range;
use function serialize;
use function str_starts_with;

final class ExternalSortTest extends FlowTestCase
{
    public static function random_datasets(): Generator
    {
        for ($seed = 1; $seed <= 200; $seed++) {
            yield "seed {$seed}" => [$seed];
        }
    }

    public function test_data_under_the_limit_sorts_with_zero_spills(): void
    {
        $storage = new RecordingBucketsStorage(new MemoryBuckets());

        static::assertSame(
            [1, 2, 3, 4, 5],
            IdRows::ids(ExternalSortMother::inMemory(refs('id'), $storage)->sort(
                IdRows::batches([4, 2], [5], [1, 3]),
                flow_context(config()),
            )),
        );
        static::assertSame([], $storage->appended);
        static::assertSame([], $storage->read);
    }

    public function test_data_under_the_limit_is_emitted_in_batches_of_the_configured_size(): void
    {
        $batches = iterator_to_array(
            ExternalSortMother::inMemory(refs('id'), batchSize: 2)->sort(
                IdRows::batches([5, 4, 3], [2, 1]),
                flow_context(config()),
            ),
            false,
        );

        static::assertSame([2, 2, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
    }

    public function test_data_over_the_limit_spills_and_merges(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        static::assertSame(
            [0, 1, 2, 3, 10, 11, 12, 13],
            IdRows::ids(ExternalSortMother::spilling(refs('id'), $storage)->sort(
                IdRows::batches([10, 0], [11, 1], [2, 12], [13, 3]),
                flow_context(config()),
            )),
        );
        static::assertCount(4, array_filter($storage->readBucketIds(), static fn(string $id): bool => str_starts_with(
            $id,
            'sort-',
        )));
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_descending_single_column(): void
    {
        static::assertSame(
            [4, 3, 2, 1],
            IdRows::ids(ExternalSortMother::spilling(refs(ref('id')->desc()))->sort(
                IdRows::batches([3, 1], [4, 2]),
                flow_context(config()),
            )),
        );
    }

    public function test_sorts_by_multiple_columns(): void
    {
        $input = (static function (): Generator {
            yield array_to_rows([['a' => 1, 'b' => 2], ['a' => 1, 'b' => 1]], schema(int_schema('a'), int_schema('b')));
            yield array_to_rows([['a' => 0, 'b' => 5], ['a' => 0, 'b' => 3]], schema(int_schema('a'), int_schema('b')));
        })();

        $sorted = rows(schema(int_schema('a'), int_schema('b')));

        foreach (ExternalSortMother::spilling(refs('a', 'b'))->sort($input, flow_context(config())) as $batch) {
            $sorted = $sorted->concat(new AdaptiveBackend(), $batch);
        }

        static::assertSame(
            [['a' => 0, 'b' => 3], ['a' => 0, 'b' => 5], ['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
            $sorted->toArray(),
        );
    }

    public function test_empty_input_yields_nothing_and_writes_nothing(): void
    {
        $storage = new RecordingBucketsStorage(new MemoryBuckets());

        static::assertSame(
            [],
            IdRows::ids(ExternalSortMother::spilling(refs('id'), $storage)->sort(
                IdRows::batches([], []),
                flow_context(config()),
            )),
        );
        static::assertSame([], $storage->appended);
    }

    public function test_runs_past_the_fan_in_are_reduced_to_merged_runs_first(): void
    {
        $storage = new RecordingBucketsStorage(new MemoryBuckets());

        static::assertSame(
            range(0, 19),
            IdRows::ids(ExternalSortMother::spilling(refs('id'), $storage, mergeFanIn: 2)->sort(
                IdRows::batches(...array_map(static fn(int $i): array => [($i * 2) + 1, $i * 2], range(9, 0))),
                flow_context(config()),
            )),
        );
        static::assertNotSame([], array_filter($storage->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
    }

    public function test_merged_runs_go_to_the_merge_storage(): void
    {
        $spill = new RecordingBucketsStorage(new MemoryBuckets());
        $merge = new RecordingBucketsStorage(new MemoryBuckets());

        IdRows::ids(ExternalSortMother::with(refs('id'), $spill, Unit::fromBytes(1), 2, mergeStorage: $merge)->sort(
            IdRows::batches([3], [2], [1]),
            flow_context(config()),
        ));

        static::assertSame([], array_filter($spill->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
        static::assertNotSame([], $merge->appended);
    }

    public function test_a_limit_under_the_memory_limit_keeps_the_first_rows(): void
    {
        static::assertSame(
            [1, 2],
            IdRows::ids(ExternalSortMother::inMemory(refs('id'))->sort(
                IdRows::batches([4, 2], [5], [1, 3]),
                flow_context(config()),
                2,
            )),
        );
    }

    public function test_a_limit_over_the_memory_limit_stops_every_merge_at_the_limit(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        static::assertSame(
            [0, 1, 2],
            IdRows::ids(ExternalSortMother::spilling(refs('id'), $storage, mergeFanIn: 2)->sort(
                IdRows::batches(...array_map(static fn(int $i): array => [$i + 10, $i], range(9, 0))),
                flow_context(config()),
                3,
            )),
        );

        $merged = array_filter(
            $storage->appendedRows(),
            static fn(string $id): bool => str_starts_with($id, 'sort-merge-'),
            ARRAY_FILTER_USE_KEY,
        );

        static::assertNotSame([], $merged);

        foreach ($merged as $batches) {
            static::assertLessThanOrEqual(
                3,
                array_sum(array_map(static fn(Rows $rows): int => $rows->count(), $batches)),
            );
        }
    }

    #[DataProvider('random_datasets')]
    public function test_spilled_sort_equals_one_in_memory_sort(int $seed): void
    {
        $dataset = SortDatasetMother::random($seed);
        $input = (static function () use ($dataset): Generator {
            foreach ($dataset['runs'] as $rows) {
                yield array_to_rows($rows, $dataset['schema']);
            }
        })();

        $sorted = rows($dataset['schema']);

        foreach (ExternalSortMother::spilling(
            refs(...$dataset['references']),
            mergeFanIn: 2,
            batchSize: $dataset['mergeBatchSize'],
        )->sort($input, flow_context(config())) as $batch) {
            $sorted = $sorted->concat(
                new AdaptiveBackend(),
                $batch->matchTo($dataset['schema'], new AdaptiveBackend()),
            );
        }

        // serialized, so NaN and -0.0 compare by what they are
        static::assertSame(
            serialize(
                array_to_rows(array_merge(...$dataset['runs']), $dataset['schema'])
                    ->sortBy(...$dataset['references'])
                    ->toArray(),
            ),
            serialize($sorted->toArray()),
        );
    }

    public function test_both_storages_are_cleared_when_the_first_clear_throws(): void
    {
        $spillStorage = new SpyBucketsStorage(new MemoryBuckets());
        $mergeStorage = new SpyBucketsStorage(new MemoryBuckets());
        $spill = new Buckets(new ThrowingRemoveBucketsStorage($spillStorage));
        $merge = new Buckets($mergeStorage);

        // the upstream throws during the drain, so reduce() never writes a merged run - seed one, or the
        // merge-side assertion is true whether or not the nested finally ran
        $mergeStorage->append('pre-existing', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $merge->add(new Bucket('pre-existing', 1, 0));

        $sort = new ExternalSort(
            refs('id'),
            $spill,
            $merge,
            new NativePHPRandomValueGenerator(),
            Unit::fromBytes(1),
            2,
        );

        $upstream = (static function (): Generator {
            yield from IdRows::batches([9, 8], [7, 6]);

            throw new RuntimeException('upstream failed');
        })();

        try {
            iterator_to_array($sort->sort($upstream, flow_context(config())), false);
            static::fail('The upstream failure must escape.');
        } catch (RuntimeException $escaped) {
            static::assertSame('spill clear failed', $escaped->getMessage());

            $previous = $escaped->getPrevious();

            static::assertInstanceOf(RuntimeException::class, $previous);
            static::assertSame('upstream failed', $previous->getMessage());
        }

        static::assertSame([], $mergeStorage->liveBucketIds());
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        ExternalSortMother::with(refs('id'), new MemoryBuckets(), Unit::fromGb(1), 10, 0);
    }

    public function test_throws_when_merge_fan_in_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merge fan-in must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        ExternalSortMother::with(refs('id'), new MemoryBuckets(), Unit::fromGb(1), 0);
    }
}
