<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\DataFrame;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function array_values;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\hash_repartition;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;

final class SpillingPathsTest extends FlowIntegrationTestCase
{
    public static function paths(): Generator
    {
        $rows = [];

        for ($id = 0; $id < 40; $id++) {
            $rows[] = ['id' => $id, 'g' => $id % 4];
        }

        yield 'group by' => [
            static fn(SpyBucketsStorage $storage, Unit $limit): ConfigBuilder => config_builder()->groupBy(
                hash_group_by()->storage($storage)->memoryLimit($limit)->batchSize(2)->bucketsCount(4),
            ),
            static fn(ConfigBuilder $config): DataFrame => data_frame($config->build())
                ->read(from_array($rows))
                ->batchSize(3)
                ->groupBy('g')
                ->aggregate(count(ref('id'))),
            4,
        ];

        yield 'repartition' => [
            static fn(SpyBucketsStorage $storage, Unit $limit): ConfigBuilder => config_builder()->repartition(
                hash_repartition()->storage($storage)->memoryLimit($limit)->batchSize(2)->bucketsCount(4),
            ),
            static fn(ConfigBuilder $config): DataFrame => data_frame($config->build())
                ->read(from_array($rows))
                ->batchSize(3)
                ->repartition(ref('g')),
            40,
        ];

        yield 'join' => [
            static fn(SpyBucketsStorage $storage, Unit $limit): ConfigBuilder => config_builder()->join(
                hash_join()->storage($storage)->memoryLimit($limit)->batchSize(2)->bucketsCount(4),
            ),
            static fn(ConfigBuilder $config): DataFrame => data_frame($config->build())
                ->read(from_array($rows))
                ->batchSize(3)
                ->join(
                    data_frame()->read(from_array([['g' => 0], ['g' => 1], ['g' => 2], ['g' => 3]]))->batchSize(1),
                    join_on(['g' => 'g'], 'r_'),
                ),
            40,
        ];
    }

    /**
     * @param callable(SpyBucketsStorage, Unit): ConfigBuilder $config
     * @param callable(ConfigBuilder): DataFrame $pipeline
     */
    #[DataProvider('paths')]
    public function test_under_the_limit_no_frame_is_written(callable $config, callable $pipeline, int $rows): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        static::assertCount($rows, $pipeline($config($storage, Unit::fromGb(64)))->fetch()->toArray());
        static::assertSame([], $storage->appendedRows());
    }

    /**
     * @param callable(SpyBucketsStorage, Unit): ConfigBuilder $config
     * @param callable(ConfigBuilder): DataFrame $pipeline
     */
    #[DataProvider('paths')]
    public function test_past_the_limit_every_frame_holds_a_full_batch(
        callable $config,
        callable $pipeline,
        int $rows,
    ): void {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        static::assertCount($rows, $pipeline($config($storage, Unit::fromBytes(1)))->fetch()->toArray());

        $frames = array_merge(...array_values($storage->appendedRows()));

        static::assertNotSame([], $frames);
        static::assertSame(
            array_map(static fn(): int => 2, $frames),
            array_map(static fn(Rows $frame): int => $frame->count(), $frames),
        );
    }
}
