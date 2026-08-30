<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Join\Join;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Path\Filter\KeepAll;

use function array_column;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count as count_agg;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AlgorithmOverrideTest extends FlowIntegrationTestCase
{
    public function test_cache_override_and_from_cache_override_pair_up(): void
    {
        $cache = new InMemoryCache();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->cache('report', cache: $cache)
            ->run();

        $paired = [];
        df()->read(from_cache('report', cache: $cache))->write(to_array($paired))->run();

        static::assertSame([1, 2], array_column($paired, 'id'));

        // without the override the read finds nothing and stays silent
        $unpaired = [];
        df()->read(from_cache('report'))->write(to_array($unpaired))->run();

        static::assertSame([], $unpaired);
    }

    public function test_aggregate_override_reaches_the_aggregation(): void
    {
        $pinned = new RecordingBucketsStorage(new MemoryBuckets());
        $configured = new RecordingBucketsStorage(new MemoryBuckets());

        $output = [];
        df(config_builder()->groupBy(hash_group_by()->storage($configured))->build())
            ->read(from_array([['g' => 'a'], ['g' => 'b'], ['g' => 'a']]))
            ->aggregate([count_agg(ref('g'))], hash_group_by()->storage($pinned))
            ->write(to_array($output))
            ->run();

        static::assertNotSame([], $pinned->appended);
        static::assertSame([], $configured->appended);
    }

    public function test_group_by_override_reaches_the_aggregation(): void
    {
        $pinned = new RecordingBucketsStorage(new MemoryBuckets());
        $configured = new RecordingBucketsStorage(new MemoryBuckets());

        $output = [];
        df(config_builder()->groupBy(hash_group_by()->storage($configured))->build())
            ->read(from_array([['g' => 'a'], ['g' => 'b'], ['g' => 'a']]))
            ->groupBy([ref('g')], hash_group_by()->storage($pinned))
            ->aggregate(count_agg(ref('g')))
            ->write(to_array($output))
            ->run();

        static::assertCount(2, $output);
        static::assertNotSame([], $pinned->appended);
        static::assertSame([], $configured->appended);
    }

    public function test_grouped_data_frame_aggregate_keeps_its_variadic(): void
    {
        $output = [];
        df()
            ->read(from_array([['g' => 'a', 'v' => 1], ['g' => 'a', 'v' => 2]]))
            ->groupBy([ref('g')])
            ->aggregate(count_agg(ref('g')), sum(ref('v')))
            ->write(to_array($output))
            ->run();

        static::assertCount(1, $output);
    }

    public function test_join_each_is_not_governed_by_the_join_algorithm(): void
    {
        $pinned = new RecordingBucketsStorage(new MemoryBuckets());

        $output = [];
        df(config_builder()->join(hash_join()->storage($pinned))->build())
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->joinEach(
                new class implements DataFrameFactory {
                    public function from(Rows $rows): DataFrame
                    {
                        return df()->process(rows(
                            schema(int_schema('id'), str_schema('n')),
                            row(['id' => 1, 'n' => 'a']),
                        ));
                    }
                },
                join_on(['id' => 'id'], 'joined_'),
            )
            ->write(to_array($output))
            ->run();

        static::assertCount(2, $output);
        static::assertSame([], $pinned->appended, 'joinEach must never reach a bucket storage');
    }

    public function test_join_override_reaches_the_hash_join(): void
    {
        $pinned = new RecordingBucketsStorage(new MemoryBuckets());
        $configured = new RecordingBucketsStorage(new MemoryBuckets());

        $output = [];
        df(config_builder()->join(hash_join()->storage($configured))->build())
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->join(
                df()->read(from_array([['id' => 1, 'n' => 'a']])),
                join_on(['id' => 'id'], 'joined_'),
                Join::left,
                hash_join()->storage($pinned),
            )
            ->write(to_array($output))
            ->run();

        static::assertCount(2, $output);
        static::assertNotSame([], $pinned->appended);
        static::assertSame([], $configured->appended);
    }

    public function test_override_spill_root_comes_from_the_cache_config(): void
    {
        // the configured algorithm spills to memory, so any file under the spill root proves the
        // override built its own FilesystemBuckets over $config->cache->localFilesystemCacheDir
        $output = [];
        df(config_builder()->sort(external_sort()->storage(new MemoryBuckets()))->build())
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
            ->sortBy([ref('id')], external_sort()->runSize(1)->bucketsCount(2))
            ->write(to_array($output))
            ->run();

        static::assertSame([1, 2, 3], array_column($output, 'id'));
        static::assertNotSame(
            [],
            iterator_to_array(
                $this->fs->list(path($this->cacheDir->path() . '/flow-php-sort/*'), new KeepAll()),
                false,
            ),
        );
    }

    public function test_sort_by_pins_the_algorithm_for_this_operation(): void
    {
        $pinned = new RecordingBucketsStorage(new MemoryBuckets());
        $configured = new RecordingBucketsStorage(new MemoryBuckets());

        $output = [];
        df(config_builder()->sort(external_sort()->storage($configured)->runSize(1))->build())
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
            ->sortBy([ref('id')], external_sort()->storage($pinned)->runSize(1))
            ->write(to_array($output))
            ->run();

        static::assertSame([1, 2, 3], array_column($output, 'id'));
        static::assertNotSame([], $pinned->appended);
        static::assertSame([], $configured->appended);
    }

    public function test_sort_by_without_an_algorithm_uses_the_configured_one(): void
    {
        $configured = new RecordingBucketsStorage(new MemoryBuckets());

        $output = [];
        df(config_builder()->sort(external_sort()->storage($configured)->runSize(1))->build())
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertSame([1, 2, 3], array_column($output, 'id'));
        static::assertNotSame([], $configured->appended);
    }
}
