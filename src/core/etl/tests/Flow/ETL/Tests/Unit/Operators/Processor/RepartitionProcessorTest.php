<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Operators\Processor\RepartitionProcessor;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\GroupedRows;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
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
                if ($position === $crossAt) {
                    $backend->reported = 1_000_000_000_000;
                }

                yield $batch;
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

        // the batch at $crossAt is read past the limit: it and every later batch are partitioned
        if ($crossAt < $count) {
            static::assertNotSame([], $storage->appendedRows());
        }
    }
}
