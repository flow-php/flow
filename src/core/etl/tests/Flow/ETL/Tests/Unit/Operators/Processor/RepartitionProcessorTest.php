<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Operators\Processor\RepartitionProcessor;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\GroupedRows;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RepartitionProcessorMother;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_array;
use function iterator_to_array;
use function memory_get_usage;
use function mt_rand;
use function mt_srand;
use function sort;

final class RepartitionProcessorTest extends FlowTestCase
{
    public function test_a_repartition_is_clustered_on_its_columns(): void
    {
        $distribution = (new RepartitionProcessor(
            refs('country', 'day'),
            config()->repartition,
            new NativePHPRandomValueGenerator(),
        ))->distribution();

        static::assertSame(['country', 'day'], $distribution?->keys);
        static::assertNull($distribution->partial);
    }

    public function test_a_repartition_that_does_not_describe_has_no_distribution(): void
    {
        static::assertNull(
            (new RepartitionProcessor(
                refs('id'),
                config()->repartition,
                new FixedRandomValueGenerator('fixed'),
            ))->distribution(),
        );
    }

    public function test_a_processor_with_an_undescribable_random_value_generator_does_not_describe(): void
    {
        static::assertNull(
            (new RepartitionProcessor(
                refs('id'),
                config()->repartition,
                new FixedRandomValueGenerator('fixed'),
            ))->describe(),
        );
    }

    public static function random_datasets(): Generator
    {
        for ($seed = 1; $seed <= 50; $seed++) {
            yield "seed {$seed}" => [$seed];
        }
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(str_schema('k'), int_schema('v'));

        static::assertEquals($input, RepartitionProcessorMother::inMemory(refs(ref('k')))->bind($input)->output);
    }

    public function test_under_the_limit_it_groups_in_one_pass_without_touching_the_buckets(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $schema = schema(str_schema('k'), int_schema('v'));

        static::assertSame(
            [[1, 3], [2, 4]],
            array_map(
                static fn(Rows $group): array => $group->reduceToArray('v'),
                iterator_to_array(
                    RepartitionProcessorMother::inMemory(refs(ref('k')), $storage)->process(
                        (static function () use ($schema): Generator {
                            yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2]], $schema);
                            yield array_to_rows([['k' => 'a', 'v' => 3], ['k' => 'b', 'v' => 4]], $schema);
                        })(),
                        flow_context()->backend(),
                    ),
                    false,
                ),
            ),
        );
        static::assertSame([], $storage->appendedRows());
    }

    /**
     * Past the limit: one bucket proves the grouping is the processor's own work; 64 prove a key is never split
     * across buckets, which is what makes the downstream window contiguous.
     *
     * @param int<1, max> $bucketsCount
     */
    #[TestWith([1])]
    #[TestWith([64])]
    public function test_past_the_limit_every_row_sharing_a_key_still_arrives_in_one_batch(int $bucketsCount): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $schema = schema(str_schema('k'), int_schema('v'));

        $groups = array_map(
            static fn(Rows $group): array => $group->reduceToArray('v'),
            iterator_to_array(
                RepartitionProcessorMother::with(refs(ref('k')), $storage, Unit::fromBytes(1), $bucketsCount)->process(
                    (static function () use ($schema): Generator {
                        yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2]], $schema);
                        yield array_to_rows([['k' => 'a', 'v' => 3], ['k' => 'b', 'v' => 4]], $schema);
                    })(),
                    flow_context()->backend(),
                ),
                false,
            ),
        );
        sort($groups);

        static::assertSame([[1, 3], [2, 4]], $groups);
        static::assertNotSame([], $storage->appendedRows());
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_past_the_limit_the_rows_spread_over_no_more_buckets_than_configured(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $schema = schema(str_schema('k'), int_schema('v'));

        iterator_to_array(
            RepartitionProcessorMother::with(refs(ref('k')), $storage, Unit::fromBytes(1), 2)->process(
                (static function () use ($schema): Generator {
                    yield array_to_rows([
                        ['k' => 'a', 'v' => 1],
                        ['k' => 'b', 'v' => 2],
                        ['k' => 'c', 'v' => 3],
                        ['k' => 'd', 'v' => 4],
                    ], $schema);
                    yield array_to_rows([['k' => 'e', 'v' => 5], ['k' => 'f', 'v' => 6]], $schema);
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertNotSame([], $storage->appendedRows());
        static::assertLessThanOrEqual(2, count($storage->appendedRows()));
    }

    public function test_an_empty_stream_yields_no_group(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        static::assertSame(
            [],
            iterator_to_array(
                RepartitionProcessorMother::with(refs(ref('k')), $storage, Unit::fromBytes(1))->process(
                    (static fn(): Generator => yield from [])(),
                    flow_context()->backend(),
                ),
                false,
            ),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_memory_held_before_the_repartition_keeps_it_in_memory(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $schema = schema(str_schema('k'), int_schema('v'));
        $backend = new ReportedMemoryBackend(2_000_000_000);

        static::assertSame(
            [[1, 3], [2, 4]],
            array_map(
                static fn(Rows $group): array => $group->reduceToArray('v'),
                iterator_to_array(
                    RepartitionProcessorMother::with(refs(ref('k')), $storage, Unit::fromGb(1))->process(
                        (static function () use ($schema): Generator {
                            yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2]], $schema);
                            yield array_to_rows([['k' => 'a', 'v' => 3], ['k' => 'b', 'v' => 4]], $schema);
                        })(),
                        $backend,
                    ),
                    false,
                ),
            ),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_what_the_upstream_allocates_on_its_first_pull_is_not_the_repartitions_own(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $schema = schema(str_schema('k'), int_schema('v'));
        $backend = new ReportedMemoryBackend(0);

        static::assertSame(
            [[1, 3], [2, 4]],
            array_map(
                static fn(Rows $group): array => $group->reduceToArray('v'),
                iterator_to_array(
                    RepartitionProcessorMother::with(refs(ref('k')), $storage, Unit::fromGb(1))->process(
                        (static function () use ($backend, $schema): Generator {
                            // a lazy upstream, a join build for one, allocates before its first batch
                            $backend->reported = 2_000_000_000;

                            yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2]], $schema);
                            yield array_to_rows([['k' => 'a', 'v' => 3], ['k' => 'b', 'v' => 4]], $schema);
                        })(),
                        $backend,
                    ),
                    false,
                ),
            ),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_the_storage_counts_the_rows_the_bounded_read_holds(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        $schema = schema(str_schema('k'), int_schema('v'));
        $backend = new ReportedMemoryBackend(0);

        $groups = array_map(
            static fn(Rows $group): array => $group->reduceToArray('v'),
            iterator_to_array(
                RepartitionProcessorMother::with(refs(ref('k')), $storage, Unit::fromBytes(1_000_000_000))->process(
                    (static function () use ($backend, $schema): Generator {
                        // every batch is held for the rest of the run
                        foreach ([[['a', 1], ['b', 2]], [['a', 3], ['b', 4]], [['a', 5]]] as $batch) {
                            $backend->reported += 600_000_000;

                            yield array_to_rows(array_map(static fn(array $row): array => [
                                'k' => $row[0],
                                'v' => $row[1],
                            ], $batch), $schema);
                        }
                    })(),
                    $backend,
                ),
                false,
            ),
        );
        sort($groups);

        static::assertSame([[2, 4], [1, 3, 5]], $groups);
        static::assertNotSame([], $storage->appendedRows());
    }

    #[DataProvider('random_datasets')]
    public function test_crossing_the_limit_at_any_batch_gives_the_groups_one_in_memory_pass_gives(int $seed): void
    {
        mt_srand($seed);
        $schema = schema(int_schema('k', nullable: true), int_schema('v'));
        $batches = [];
        $id = 0;

        for ($b = 0, $count = mt_rand(1, 8); $b < $count; $b++) {
            $rows = [];

            for ($r = 0, $size = mt_rand(1, 20); $r < $size; $r++) {
                $rows[] = ['k' => mt_rand(0, 6) === 0 ? null : mt_rand(0, 40), 'v' => $id++];
            }

            $batches[] = array_to_rows($rows, $schema);
        }

        $crossAt = mt_rand(0, $count);
        $backend = new ReportedMemoryBackend();
        $input = (static function () use ($batches, $crossAt, $backend): Generator {
            foreach ($batches as $position => $batch) {
                yield $batch;

                if ($position === $crossAt) {
                    $backend->reported = 1_000_000_000_000;
                }
            }
        })();

        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        static::assertSame(
            GroupedRows::batchesAsSet(RepartitionProcessorMother::inMemory(refs(ref('k')))->process(
                (static fn(): Generator => yield from $batches)(),
                flow_context()->backend(),
            )),
            GroupedRows::batchesAsSet(RepartitionProcessorMother::with(
                refs(ref('k')),
                $storage,
                Unit::fromBytes(memory_get_usage(false) + 500_000_000),
            )->process($input, flow_context(config_builder()->backend($backend)->build())->backend())),
        );

        // the limit is passed once the batch at $crossAt was read, so the read batches and the rest are partitioned
        if ($crossAt < $count) {
            static::assertNotSame([], $storage->appendedRows());
        }
    }

    public function test_rebuilds_with_the_rebuild_progress(): void
    {
        $spy = new ProgressSpy();

        iterator_to_array(
            DescribeRoundTrip::rebuilt(
                new RepartitionProcessor(refs('id'), config()->repartition, new NativePHPRandomValueGenerator()),
                new Rebuild($spy),
            )->process(
                (static function (): Generator {
                    yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(['repartition'], $spy->stages());
        static::assertGreaterThan(0, $spy->countOf(StageName::repartition));
    }

    public function test_a_window_repartition_rebuilds_with_the_window_stage(): void
    {
        $spy = new ProgressSpy();

        DescribeRoundTrip::rebuilt(
            new RepartitionProcessor(
                refs('id'),
                config()->repartition,
                new NativePHPRandomValueGenerator(),
                stage: StageName::window,
            ),
            new Rebuild($spy),
        );

        static::assertSame(['window'], $spy->stages());
    }

    public function test_a_description_without_a_stage_rebuilds_with_the_repartition_stage(): void
    {
        $spy = new ProgressSpy();
        $normalized = type_array()->assert(
            (new RepartitionProcessor(refs('id'), config()->repartition, new NativePHPRandomValueGenerator()))
                ->describe()
                ?->normalize(),
        );
        $data = type_array()->assert($normalized['data'] ?? null);
        unset($data['stage']);

        (new Rebuild($spy))->of([...$normalized, 'data' => $data], RepartitionProcessor::class);

        static::assertSame(['repartition'], $spy->stages());
    }

    #[TestWith([64 * 1024 * 1024 * 1024])]
    #[TestWith([1])]
    public function test_counts_every_row_of_every_group_it_yields(int $memoryLimitBytes): void
    {
        $progress = new ProgressSpy();
        $schema = schema(str_schema('k'), int_schema('v'));

        iterator_to_array(
            RepartitionProcessorMother::with(
                refs(ref('k')),
                new MemoryRowsStorage(),
                Unit::fromBytes($memoryLimitBytes),
                progress: $progress->stage(StageName::repartition),
            )->process(
                (static function () use ($schema): Generator {
                    yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2]], $schema);
                    yield array_to_rows([['k' => 'a', 'v' => 3], ['k' => 'b', 'v' => 4]], $schema);
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(4, $progress->countOf(StageName::repartition));
    }

    public function test_repartition_processor_describes_its_progress(): void
    {
        $spy = new ProgressSpy();

        DescribeRoundTrip::rebuilt(
            new RepartitionProcessor(
                refs('id'),
                config()->repartition,
                new NativePHPRandomValueGenerator(),
                HeartbeatContext::stageWithId(StageName::window, 7),
                StageName::window,
            ),
            new Rebuild($spy),
        );

        static::assertSame([['window', ProgressUnit::rows, 7, null]], $spy->built());
    }

    public function test_repartition_processor_is_done_after_its_last_batch(): void
    {
        $progress = new ProgressSpy();
        $output = (new RepartitionProcessor(
            refs('id'),
            config()->repartition,
            new NativePHPRandomValueGenerator(),
            $progress->stage(StageName::repartition),
        ))->process(
            (static function (): Generator {
                yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        );

        $output->current();

        static::assertSame(0, $progress->doneCalls(StageName::repartition));

        iterator_to_array($output, false);

        static::assertSame(1, $progress->doneCalls(StageName::repartition));
    }

    public function test_its_distribution_holds_the_stage_it_reports_to(): void
    {
        $stage = new ProgressSpy(StageName::repartition);

        static::assertSame(
            $stage,
            (new RepartitionProcessor(
                refs('country', 'day'),
                config()->repartition,
                new NativePHPRandomValueGenerator(),
                $stage,
            ))->distribution()?->progress,
        );
    }

    public function test_a_repartition_past_its_memory_limit_is_done_once(): void
    {
        $progress = new ProgressSpy();

        iterator_to_array(
            RepartitionProcessorMother::with(
                refs(ref('id')),
                new MemoryRowsStorage(),
                Unit::fromBytes(1),
                progress: $progress->stage(StageName::repartition),
            )->process(
                (static function (): Generator {
                    yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(1, $progress->doneCalls(StageName::repartition));
    }
}
