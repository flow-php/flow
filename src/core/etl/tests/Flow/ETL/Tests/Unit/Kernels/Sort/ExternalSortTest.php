<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Sort;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Kernels\Bucketing\Bucket;
use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Kernels\Sort\ExternalSort;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\Context\SortedRows;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\Double\RecordingRowsStorage;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\Double\ThrowingRemoveRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Tests\Mother\SortDatasetMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\MemoryBudget;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use WeakReference;

use function array_chunk;
use function array_filter;
use function array_map;
use function array_merge;
use function array_sum;
use function array_unique;
use function array_values;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function memory_get_usage;
use function range;
use function serialize;
use function str_starts_with;

use const ARRAY_FILTER_USE_KEY;

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
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = config()->backend();
        $sort = ExternalSortMother::with([ref('id')->asc()], $storage);

        static::assertSame(
            [1, 2, 3, 4, 5],
            IdRows::ids($sort->sort(
                IdRows::batches([4, 2], [5], [1, 3]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), null),
            )),
        );
        static::assertSame([], $storage->appended);
        static::assertSame([], $storage->read);
    }

    public function test_memory_held_before_the_sort_does_not_shrink_its_runs(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend(2_000_000_000);
        $sort = ExternalSortMother::with([ref('id')->asc()], $storage);

        static::assertSame(
            [1, 2, 3, 4, 5],
            IdRows::ids($sort->sort(
                IdRows::batches([4, 2], [5], [1, 3]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), null),
            )),
        );
        static::assertSame([], $storage->appended);
    }

    public function test_data_under_the_limit_is_emitted_in_batches_of_the_configured_size(): void
    {
        $backend = config()->backend();
        $sort = ExternalSortMother::with([ref('id')->asc()], batchSize: 2);

        static::assertSame(
            [2, 2, 1],
            array_map(
                static fn(Rows $rows): int => $rows->count(),
                iterator_to_array(
                    $sort->sort(
                        IdRows::batches([5, 4, 3], [2, 1]),
                        $backend,
                        new MemoryBudget($backend, Unit::fromGb(1), null),
                    ),
                    false,
                ),
            ),
        );
    }

    public function test_the_last_run_spilled_after_the_input_ends_returns_freed_memory_too(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $budget = new MemoryBudget($backend, Unit::fromBytes(1_000_000), null);
        $rows = (static function () use ($backend): Generator {
            // the first batch alone passes the limit, so it spills in the loop; the second stays under it
            $backend->reported = 600_000;

            yield from IdRows::batches([4, 2]);
            yield from IdRows::batches([3, 1]);
        })();

        static::assertSame(
            [1, 2, 3, 4],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->sort($rows, $backend, $budget)),
        );
        static::assertCount(2, array_unique($storage->appended));
        static::assertSame(2, $backend->releases);
    }

    public function test_data_over_the_limit_spills_and_merges(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = 0;
        $sort = ExternalSortMother::with([ref('id')->asc()], $storage);

        static::assertSame(
            [0, 1, 2, 3, 10, 11, 12, 13],
            IdRows::ids($sort->sort(
                IdRows::reportedFrames($backend, $yields, [10, 0], [11, 1], [2, 12], [13, 3]),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
            )),
        );
        static::assertCount(4, array_filter(
            $storage->readBucketIds(),
            static fn(string $id): bool => str_starts_with($id, 'sort-') && !str_starts_with($id, 'sort-merge-'),
        ));
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_a_final_sort_under_the_cap_stays_in_memory(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $cap = Unit::fromGb(10);
        // 100 MB under the cap, and the six batches' sort needs 24 MB
        $backend = new ReportedMemoryBackend(
            $cap->inBytes() - memory_get_usage(false) - Unit::fromMb(100)->inBytes(),
            sortWorkingBytes: Unit::fromMb(4)->inBytes(),
        );

        static::assertSame(
            [1, 2, 3, 4, 5, 6],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->sort(
                IdRows::batches([6], [3], [5], [1], [4], [2]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), $cap),
            )),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_a_final_sort_that_would_cross_the_cap_spills_in_runs_that_fit(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $cap = Unit::fromGb(10);
        // 10 MB under the cap and 4 MB to sort each batch: two batches fit in a run
        $backend = new ReportedMemoryBackend(
            $cap->inBytes() - memory_get_usage(false) - Unit::fromMb(10)->inBytes(),
            sortWorkingBytes: Unit::fromMb(4)->inBytes(),
        );

        static::assertSame(
            [1, 2, 3, 4, 5, 6],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->sort(
                IdRows::batches([6], [3], [5], [1], [4], [2]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), $cap),
            )),
        );
        static::assertCount(3, array_filter(
            $storage->readBucketIds(),
            static fn(string $id): bool => str_starts_with($id, 'sort-') && !str_starts_with($id, 'sort-merge-'),
        ));
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_a_batch_whose_sort_alone_would_cross_the_cap_is_a_run_of_its_own(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $cap = Unit::fromGb(10);
        // 1 MB under the cap and 4 MB to sort each batch: no two batches fit, one still has to make progress
        $backend = new ReportedMemoryBackend(
            $cap->inBytes() - memory_get_usage(false) - Unit::fromMb(1)->inBytes(),
            sortWorkingBytes: Unit::fromMb(4)->inBytes(),
        );

        static::assertSame(
            [1, 2, 3],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->sort(
                IdRows::batches([3], [1], [2]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), $cap),
            )),
        );
        static::assertCount(3, array_filter(
            $storage->readBucketIds(),
            static fn(string $id): bool => str_starts_with($id, 'sort-') && !str_starts_with($id, 'sort-merge-'),
        ));
    }

    public function test_a_sort_under_the_floor_stays_in_memory_while_the_process_is_over_the_cap(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $cap = Unit::fromGb(10);
        $backend = new ReportedMemoryBackend($cap->inBytes() - memory_get_usage(false) + Unit::fromMb(100)->inBytes());

        static::assertSame(
            [1, 2, 3],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->sort(
                IdRows::batches([3], [1], [2]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), $cap),
            )),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_a_sort_over_the_floor_spills_runs_of_the_floor_while_the_process_is_over_the_cap(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $cap = Unit::fromGb(10);
        // the floor is 128 MiB and each batch needs 50 MB to sort: two batches fit in a run
        $backend = new ReportedMemoryBackend(
            $cap->inBytes() - memory_get_usage(false) + Unit::fromMb(100)->inBytes(),
            sortWorkingBytes: Unit::fromMb(50)->inBytes(),
        );

        static::assertSame(
            [1, 2, 3, 4, 5, 6],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->sort(
                IdRows::batches([6], [3], [5], [1], [4], [2]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), $cap),
            )),
        );
        static::assertCount(3, array_filter(
            $storage->readBucketIds(),
            static fn(string $id): bool => str_starts_with($id, 'sort-') && !str_starts_with($id, 'sort-merge-'),
        ));
    }

    public function test_a_spill_whose_sort_would_cross_the_cap_writes_runs_that_fit(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $cap = Unit::fromGb(10);
        // 30 MB under the cap and 4 MB to sort each batch
        $backend = new ReportedMemoryBackend(
            $cap->inBytes() - memory_get_usage(false) - Unit::fromMb(30)->inBytes(),
            sortWorkingBytes: Unit::fromMb(4)->inBytes(),
        );
        $batches = (static function () use ($backend): Generator {
            yield from IdRows::batches([6], [3], [5], [1]);
            // twice 11 MB passes the 20 MB limit and leaves 19 MB under the cap: four of the five batches fit
            $backend->reported += Unit::fromMb(11)->inBytes();

            yield from IdRows::batches([4]);
            yield from IdRows::batches([2]);
        })();

        static::assertSame(
            [1, 2, 3, 4, 5, 6],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->sort(
                $batches,
                $backend,
                new MemoryBudget($backend, Unit::fromMb(20), $cap),
            )),
        );
        static::assertCount(3, array_filter(
            $storage->readBucketIds(),
            static fn(string $id): bool => str_starts_with($id, 'sort-') && !str_starts_with($id, 'sort-merge-'),
        ));
    }

    public function test_a_spilled_run_is_written_in_frames_of_at_most_the_batch_size(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = 0;
        $sort = ExternalSortMother::with([ref('id')->asc()], $storage, batchSize: 2);

        static::assertSame(
            [0, 1, 2, 3, 4, 10, 11, 12, 13, 14],
            IdRows::ids($sort->sort(
                IdRows::reportedFrames($backend, $yields, [14, 10, 12, 11, 13], [4, 0, 2, 1, 3]),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
            )),
        );
        static::assertSame(
            [[2, 2, 1], [2, 2, 1]],
            array_values(array_filter(
                $storage->rowCounts,
                static fn(string $bucket): bool => str_starts_with($bucket, 'sort-'),
                ARRAY_FILTER_USE_KEY,
            )),
        );
    }

    public function test_descending_single_column(): void
    {
        $backend = config()->backend();
        $sort = ExternalSortMother::with([ref('id')->desc()]);

        static::assertSame(
            [4, 3, 2, 1],
            IdRows::ids($sort->sort(
                IdRows::batches([3, 1], [4, 2]),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
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
        $backend = config()->backend();
        $sort = ExternalSortMother::with([ref('a')->asc(), ref('b')->asc()]);

        foreach ($sort->sort($input, $backend, new MemoryBudget($backend, Unit::fromBytes(1), null)) as $batch) {
            $sorted = Rows::concat($sorted->schema(), new AdaptiveBackend(), $sorted, $batch);
        }

        static::assertSame(
            [['a' => 0, 'b' => 3], ['a' => 0, 'b' => 5], ['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2]],
            $sorted->toArray(),
        );
    }

    public function test_empty_input_yields_nothing_and_writes_nothing(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = config()->backend();
        $sort = ExternalSortMother::with([ref('id')->asc()], $storage);

        static::assertSame(
            [],
            IdRows::ids($sort->sort(
                IdRows::batches([], []),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
            )),
        );
        static::assertSame([], $storage->appended);
    }

    public function test_an_in_memory_sort_counts_every_row_it_takes_once(): void
    {
        $progress = new ProgressSpy();
        $backend = config()->backend();
        $sort = ExternalSortMother::with([ref('id')->asc()], progress: $progress->stage(StageName::sort));

        IdRows::ids($sort->sort(
            $sort->counted(IdRows::batches([10, 0], [11, 1], [2, 12], [13, 3])),
            $backend,
            new MemoryBudget($backend, Unit::fromGb(1), null),
        ));

        static::assertSame(8, $progress->countOf(StageName::sort));
        static::assertGreaterThan(0, $progress->busyCalls(StageName::sort));
    }

    public function test_a_spilling_sort_counts_every_row_it_takes_once_and_is_busy_while_it_merges(): void
    {
        $progress = new ProgressSpy();
        $backend = new ReportedMemoryBackend();
        $yields = 0;
        $sort = ExternalSortMother::with(
            [ref('id')->asc()],
            mergeFanIn: 2,
            progress: $progress->stage(StageName::sort),
        );

        IdRows::ids($sort->sort(
            $sort->counted(IdRows::reportedFrames(
                $backend,
                $yields,
                ...array_map(static fn(int $i): array => [($i * 2) + 1, $i * 2], range(9, 0)),
            )),
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(1), null),
        ));

        // spills, three merge passes (10 -> 5 -> 3 -> 2 runs) and the output only report that the sort is busy
        static::assertSame(20, $progress->countOf(StageName::sort));
        static::assertGreaterThan(20, $progress->busyCalls(StageName::sort));
    }

    public function test_runs_past_the_fan_in_are_reduced_to_merged_runs_first(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = 0;
        $sort = ExternalSortMother::with([ref('id')->asc()], $storage, mergeFanIn: 2);

        static::assertSame(
            range(0, 19),
            IdRows::ids($sort->sort(
                IdRows::reportedFrames(
                    $backend,
                    $yields,
                    ...array_map(static fn(int $i): array => [($i * 2) + 1, $i * 2], range(9, 0)),
                ),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
            )),
        );
        static::assertNotSame([], array_filter($storage->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
    }

    public function test_merged_runs_go_to_the_merge_storage(): void
    {
        $spill = new RecordingRowsStorage(new MemoryRowsStorage());
        $merge = new RecordingRowsStorage(new MemoryRowsStorage());

        $backend = new ReportedMemoryBackend();
        $yields = 0;
        $sort = ExternalSortMother::with([ref('id')->asc()], $spill, 2, mergeStorage: $merge);

        IdRows::ids($sort->sort(
            IdRows::reportedFrames($backend, $yields, [3], [2], [1]),
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(1), null),
        ));

        static::assertSame([], array_filter($spill->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
        static::assertNotSame([], $merge->appended);
    }

    public function test_a_limit_under_the_memory_limit_keeps_the_first_rows(): void
    {
        $backend = config()->backend();
        $sort = ExternalSortMother::with([ref('id')->asc()]);

        static::assertSame(
            [1, 2],
            IdRows::ids($sort->sort(
                IdRows::batches([4, 2], [5], [1, 3]),
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), null),
                2,
            )),
        );
    }

    public function test_a_limit_over_the_memory_limit_stops_every_merge_at_the_limit(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = 0;
        $sort = ExternalSortMother::with([ref('id')->asc()], $storage, mergeFanIn: 2);

        static::assertSame(
            [0, 1, 2],
            IdRows::ids($sort->sort(
                IdRows::reportedFrames(
                    $backend,
                    $yields,
                    ...array_map(static fn(int $i): array => [$i + 10, $i], range(9, 0)),
                ),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
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
        $backend = new ReportedMemoryBackend();
        $input = (static function () use ($dataset, $backend): Generator {
            foreach ($dataset['runs'] as $rows) {
                $backend->reported += 1_000_000;

                yield array_to_rows($rows, $dataset['schema']);
            }
        })();

        $sorted = rows($dataset['schema']);
        $sort = ExternalSortMother::with($dataset['columns'], mergeFanIn: 2, batchSize: $dataset['mergeBatchSize']);

        foreach ($sort->sort($input, $backend, new MemoryBudget($backend, Unit::fromBytes(1), null)) as $batch) {
            $sorted = Rows::concat(
                $sorted->schema(),
                new AdaptiveBackend(),
                $sorted,
                $batch->matchTo($dataset['schema'], new AdaptiveBackend()),
            );
        }

        // serialized, so NaN and -0.0 compare by what they are
        static::assertSame(
            serialize(
                SortedRows::of(
                    array_to_rows(array_merge(...$dataset['runs']), $dataset['schema']),
                    ...$dataset['columns'],
                )->toArray(),
            ),
            serialize($sorted->toArray()),
        );
    }

    public function test_both_storages_are_cleared_when_the_first_clear_throws(): void
    {
        $spillStorage = new SpyRowsStorage(new MemoryRowsStorage());
        $mergeStorage = new SpyRowsStorage(new MemoryRowsStorage());
        $spill = new Buckets(new ThrowingRemoveRowsStorage($spillStorage));
        $merge = new Buckets($mergeStorage);

        // the upstream throws during the drain, so reduce() never writes a merged run - seed one, or the
        // merge-side assertion is true whether or not the nested finally ran
        $mergeStorage->append('pre-existing', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $merge->add(new Bucket('pre-existing', 1, 0));

        $backend = new ReportedMemoryBackend();

        try {
            iterator_to_array(
                (new ExternalSort([ref('id')->asc()], $spill, $merge, new NativePHPRandomValueGenerator(), 2))->sort(
                    (static function () use ($backend): Generator {
                        $yields = 0;

                        yield from IdRows::reportedFrames($backend, $yields, [9, 8], [7, 6]);

                        throw new RuntimeException('upstream failed');
                    })(),
                    $backend,
                    new MemoryBudget($backend, Unit::fromBytes(1), null),
                ),
                false,
            );
            static::fail('The upstream failure must escape.');
        } catch (RuntimeException $escaped) {
            static::assertSame('spill clear failed', $escaped->getMessage());

            $previous = $escaped->getPrevious();

            static::assertInstanceOf(RuntimeException::class, $previous);
            static::assertSame('upstream failed', $previous->getMessage());
        }

        static::assertSame([], $mergeStorage->liveBucketIds());
    }

    public function test_throws_when_merge_fan_in_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Merge fan-in must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        ExternalSortMother::with([ref('id')->asc()], new MemoryRowsStorage(), 0);
    }

    public function test_of_merges_into_the_spill_storage_without_a_merge_storage(): void
    {
        $spill = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = 0;

        iterator_to_array(
            ExternalSort::of(
                [ref('id')->asc()],
                external_sort()
                    ->storage($spill)
                    ->memoryLimit(Unit::fromBytes(1))
                    ->bucketsCount(2)
                    ->build(path('memory://spill'), $backend),
                new NativePHPRandomValueGenerator(),
            )->sort(
                IdRows::reportedFrames(
                    $backend,
                    $yields,
                    ...array_map(static fn(int $id): array => [$id], range(9, 0)),
                ),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
            ),
            false,
        );

        static::assertNotSame([], array_filter($spill->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
    }

    public function test_of_merges_into_the_merge_storage_when_set(): void
    {
        $spill = new RecordingRowsStorage(new MemoryRowsStorage());
        $merge = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = 0;

        iterator_to_array(
            ExternalSort::of(
                [ref('id')->asc()],
                external_sort()
                    ->storage($spill)
                    ->mergeStorage($merge)
                    ->memoryLimit(Unit::fromBytes(1))
                    ->bucketsCount(2)
                    ->build(path('memory://spill'), $backend),
                new NativePHPRandomValueGenerator(),
            )->sort(
                IdRows::reportedFrames(
                    $backend,
                    $yields,
                    ...array_map(static fn(int $id): array => [$id], range(9, 0)),
                ),
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(1), null),
            ),
            false,
        );

        static::assertSame([], array_filter($spill->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
        static::assertNotSame([], array_filter($merge->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
    }

    public function test_merge_gives_the_rows_of_every_stream_in_order(): void
    {
        $backend = new PhpBackend();

        static::assertSame(
            [1, 2, 3, 4, 5, 6, 7, 8],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()])->merge(
                [IdRows::batches([1, 4], [7]), IdRows::batches([2, 5]), IdRows::batches([3, 6], [8])],
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), null),
                new BatchSize(3),
            )),
        );
    }

    public function test_merge_of_no_stream_and_of_empty_streams_gives_nothing(): void
    {
        $backend = new PhpBackend();
        $sort = ExternalSortMother::with([ref('id')->asc()]);

        static::assertSame(
            [],
            IdRows::ids($sort->merge(
                [],
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), null),
                new BatchSize(3),
            )),
        );
        static::assertSame(
            [],
            IdRows::ids($sort->merge(
                [IdRows::batches(), IdRows::batches()],
                $backend,
                new MemoryBudget($backend, Unit::fromGb(1), null),
                new BatchSize(3),
            )),
        );
    }

    public function test_merge_holds_the_frames_its_budget_allows_for_each_stream(): void
    {
        $backend = new ReportedMemoryBackend();
        $yields = [0, 0, 0, 0];
        $streams = [
            IdRows::reportedFrames($backend, $yields[0], [0], [1], [2], [3], [4]),
            IdRows::reportedFrames($backend, $yields[1], [10], [11], [12], [13], [14]),
            IdRows::reportedFrames($backend, $yields[2], [20], [21], [22], [23], [24]),
            IdRows::reportedFrames($backend, $yields[3], [30], [31], [32], [33], [34]),
        ];

        ExternalSortMother::with([ref('id')->asc()])->merge(
            $streams,
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(28_000_000), null),
            new BatchSize(1),
        )->current();

        static::assertSame(3, $yields[3]);
    }

    public function test_merge_gives_the_streams_half_of_its_budget(): void
    {
        $backend = new ReportedMemoryBackend();
        $yields = [0, 0, 0, 0];
        $streams = [
            IdRows::reportedFrames($backend, $yields[0], [0], [1], [2], [3], [4], [5], [6], [7]),
            IdRows::reportedFrames($backend, $yields[1], [10], [11], [12], [13], [14], [15], [16], [17]),
            IdRows::reportedFrames($backend, $yields[2], [20], [21], [22], [23], [24], [25], [26], [27]),
            IdRows::reportedFrames($backend, $yields[3], [30], [31], [32], [33], [34], [35], [36], [37]),
        ];

        ExternalSortMother::with([ref('id')->asc()])->merge(
            $streams,
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(28_000_000), null),
            new BatchSize(1),
        )->current();

        static::assertSame(3, $yields[3]);
    }

    public function test_coalesced_releases_its_frames_before_it_hands_the_batch_over(): void
    {
        $frames = [];
        $rows = (static function () use (&$frames): Generator {
            foreach ([1, 2, 3, 4] as $id) {
                $frame = array_to_rows([['id' => $id]], schema(int_schema('id')));
                $frames[] = WeakReference::create($frame);

                yield $frame;
            }
        })();

        $coalesced = ExternalSort::coalesced($rows, 3, new PhpBackend());
        $coalesced->current();

        static::assertLessThanOrEqual(
            1,
            count(array_filter($frames, static fn(WeakReference $frame): bool => $frame->get() !== null)),
        );
    }

    public function test_merge_holds_one_frame_for_each_stream_when_no_more_fits(): void
    {
        $backend = new ReportedMemoryBackend();
        $yields = [0, 0, 0, 0];
        $streams = [
            IdRows::reportedFrames($backend, $yields[0], [0], [1], [2], [3], [4]),
            IdRows::reportedFrames($backend, $yields[1], [10], [11], [12], [13], [14]),
            IdRows::reportedFrames($backend, $yields[2], [20], [21], [22], [23], [24]),
            IdRows::reportedFrames($backend, $yields[3], [30], [31], [32], [33], [34]),
        ];

        ExternalSortMother::with([ref('id')->asc()])->merge(
            $streams,
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(9_000_000), null),
            new BatchSize(1),
        )->current();

        static::assertSame(1, $yields[3]);
    }

    public function test_merge_never_holds_more_than_the_cursor_rows_for_a_stream(): void
    {
        $backend = new ReportedMemoryBackend();
        $yields = [0, 0];
        $streams = [
            IdRows::reportedFrames($backend, $yields[0], ...array_chunk(range(0, 24_575), 8_192)),
            IdRows::reportedFrames($backend, $yields[1], ...array_chunk(range(100_000, 124_575), 8_192)),
        ];

        ExternalSortMother::with([ref('id')->asc()])->merge(
            $streams,
            $backend,
            new MemoryBudget($backend, Unit::fromGb(1), null),
            new BatchSize(1),
        )->current();

        static::assertSame(2, $yields[1]);
    }

    public function test_streams_that_do_not_fit_with_one_frame_each_are_merged_in_groups_first(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = [0, 0, 0, 0, 0];

        static::assertSame(
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->merge(
                [
                    IdRows::reportedFrames($backend, $yields[0], [0], [5]),
                    IdRows::reportedFrames($backend, $yields[1], [1], [6]),
                    IdRows::reportedFrames($backend, $yields[2], [2], [7]),
                    IdRows::reportedFrames($backend, $yields[3], [3], [8]),
                    IdRows::reportedFrames($backend, $yields[4], [4], [9]),
                ],
                $backend,
                new MemoryBudget($backend, Unit::fromBytes(5_000_000), null),
                new BatchSize(2),
            )),
        );
        static::assertNotSame([], array_filter($storage->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
    }

    public function test_the_runs_a_sort_merges_in_groups_are_numbered_in_the_order_they_are_made(): void
    {
        // scripted, so every batch spills as its own run whatever the test process holds
        $backend = new ReportedMemoryBackend();
        $yields = 0;
        $sort = ExternalSortMother::with([ref('id')->asc()], mergeFanIn: 2);
        $sorted = $sort->sort(
            IdRows::reportedFrames($backend, $yields, [8], [7], [6], [5], [4], [3], [2], [1]),
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(1), null),
        );
        $sorted->current();

        // the first pass made 0 to 3 and its runs are merged again, so the runs that remain are the second pass's
        static::assertSame([4, 5], array_map(static fn(Bucket $bucket): int => $bucket->index, $sort->merge->all()));
    }

    public function test_the_streams_merge_reduces_in_groups_are_numbered_in_the_order_they_are_made(): void
    {
        $backend = new ReportedMemoryBackend();
        $yields = [0, 0, 0, 0, 0];
        $sort = ExternalSortMother::with([ref('id')->asc()]);
        $merged = $sort->merge(
            [
                IdRows::reportedFrames($backend, $yields[0], [0], [5]),
                IdRows::reportedFrames($backend, $yields[1], [1], [6]),
                IdRows::reportedFrames($backend, $yields[2], [2], [7]),
                IdRows::reportedFrames($backend, $yields[3], [3], [8]),
                IdRows::reportedFrames($backend, $yields[4], [4], [9]),
            ],
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(5_000_000), null),
            new BatchSize(2),
        );
        $merged->current();

        static::assertSame(
            [0, 1, 2, 3, 4],
            array_map(static fn(Bucket $bucket): int => $bucket->index, $sort->merge->all()),
        );
    }

    public function test_merge_removes_the_runs_it_merged(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $backend = new ReportedMemoryBackend();
        $yields = [0, 0, 0, 0, 0];

        IdRows::ids(ExternalSortMother::with([ref('id')->asc()], $storage)->merge(
            [
                IdRows::reportedFrames($backend, $yields[0], [0], [5]),
                IdRows::reportedFrames($backend, $yields[1], [1], [6]),
                IdRows::reportedFrames($backend, $yields[2], [2], [7]),
                IdRows::reportedFrames($backend, $yields[3], [3], [8]),
                IdRows::reportedFrames($backend, $yields[4], [4], [9]),
            ],
            $backend,
            new MemoryBudget($backend, Unit::fromBytes(2_500_000), null),
            new BatchSize(2),
        ));

        static::assertNotSame(
            [],
            array_filter($storage->readBucketIds(), static fn(string $id): bool => str_starts_with($id, 'sort-merge-')),
        );
        static::assertSame(
            [],
            array_filter($storage->liveBucketIds(), static fn(string $id): bool => str_starts_with($id, 'sort-merge-')),
        );
    }

    public function test_coalesced_joins_batches_up_to_the_size_and_flushes_the_rest(): void
    {
        static::assertSame(
            [ExternalSort::MERGE_CURSOR_ROWS, 2],
            array_map(
                static fn(Rows $rows): int => $rows->count(),
                iterator_to_array(
                    ExternalSort::coalesced(
                        IdRows::batches([1], range(2, ExternalSort::MERGE_CURSOR_ROWS), [0], [-1]),
                        ExternalSort::MERGE_CURSOR_ROWS,
                        new PhpBackend(),
                    ),
                    false,
                ),
            ),
        );
        static::assertSame(
            [],
            iterator_to_array(
                ExternalSort::coalesced(IdRows::batches(), ExternalSort::MERGE_CURSOR_ROWS, new PhpBackend()),
                false,
            ),
        );
    }
}
