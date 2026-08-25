<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function array_map;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\collect_unique;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\uuid_schema;
use function iterator_to_array;

final class GroupByAggregationTest extends FlowIntegrationTestCase
{
    public function test_partition_count_does_not_affect_the_result(): void
    {
        $input = [
            ['seller' => 'a', 'amount' => 10],
            ['seller' => 'b', 'amount' => 5],
            ['seller' => 'a', 'amount' => 20],
            ['seller' => 'c', 'amount' => 1],
            ['seller' => 'b', 'amount' => 7],
            ['seller' => 'a', 'amount' => 3],
        ];

        $pipeline = static fn(ConfigBuilder $config): array => iterator_to_array(
            data_frame($config)
                ->read(from_array($input))
                ->groupBy([ref('seller')])
                ->aggregate(count(ref('seller')), sum(ref('amount')))
                ->sortBy([ref('seller')->asc()])
                ->getEachAsArray(),
        );

        $default = $pipeline(config_builder());
        $fewPartitions = $pipeline(config_builder()->groupBy(hash_group_by()->bucketsCount(3)->batchSize(2)));

        static::assertSame($default, $fewPartitions);
        static::assertCount(3, $default);

        $bySeller = [];

        foreach ($default as $row) {
            $bySeller[$row['seller']] = ['count' => $row['seller_count'], 'sum' => $row['amount_sum']];
        }

        static::assertSame(
            [
                'a' => ['count' => 3, 'sum' => 33],
                'b' => ['count' => 2, 'sum' => 12],
                'c' => ['count' => 1, 'sum' => 1],
            ],
            $bySeller,
        );
    }

    public function test_multi_column_key_does_not_collide(): void
    {
        $input = [
            ['a' => 'x', 'b' => 'yz', 'v' => 1],
            ['a' => 'xy', 'b' => 'z', 'v' => 1],
            ['a' => 'x', 'b' => 'yz', 'v' => 1],
        ];

        $result = iterator_to_array(
            data_frame(config_builder()->groupBy(hash_group_by()->bucketsCount(3)))
                ->read(from_array($input))
                ->groupBy([ref('a'), ref('b')])
                ->aggregate(sum(ref('v')))
                ->sortBy([ref('a')->asc()])
                ->getEachAsArray(),
        );

        static::assertCount(2, $result);

        $byGroup = [];

        foreach ($result as $row) {
            $byGroup[(string) $row['a'] . '|' . (string) $row['b']] = $row['v_sum'];
        }

        static::assertSame(2, $byGroup['x|yz']);
        static::assertSame(1, $byGroup['xy|z']);
    }

    public function test_mixed_presence_columns_survive_the_filesystem_round_trip(): void
    {
        $input = [
            ['seller' => 'a', 'amount' => 10.5],
            ['seller' => 'a'],
            ['seller' => 'b', 'amount' => 2.5],
            ['amount' => 7.0],
        ];

        $result = iterator_to_array(
            data_frame(config_builder()->groupBy(hash_group_by()->bucketsCount(3)))
                ->read(from_array($input))
                ->groupBy([ref('seller')])
                ->aggregate(sum(ref('amount')))
                ->getEachAsArray(),
        );

        static::assertCount(3, $result);

        $bySeller = [];

        foreach ($result as $row) {
            $bySeller[$row['seller'] ?? '__null__'] = $row['amount_sum'];
        }

        static::assertSame(10.5, $bySeller['a']);
        static::assertSame(2.5, $bySeller['b']);
        // the aggregate's type follows the column, so a float column stays float
        static::assertSame(7.0, $bySeller['__null__']);
    }

    public function test_chained_filesystem_group_by_stages_do_not_corrupt_each_other(): void
    {
        $input = [
            ['seller' => 'a', 'region' => 'south'],
            ['seller' => 'b', 'region' => 'north'],
            ['seller' => 'c', 'region' => 'south'],
            ['seller' => 'd', 'region' => 'north'],
            ['seller' => 'e', 'region' => 'south'],
            ['seller' => 'f', 'region' => 'north'],
        ];

        $result = iterator_to_array(
            data_frame(config_builder()->groupBy(hash_group_by()->bucketsCount(3)))
                ->read(from_array($input))
                ->groupBy([ref('seller'), ref('region')])
                ->aggregate(count(ref('seller')))
                ->groupBy([ref('region')])
                ->aggregate(count(ref('region')))
                ->sortBy([ref('region')->asc()])
                ->getEachAsArray(),
        );

        $byRegion = [];

        foreach ($result as $row) {
            $byRegion[$row['region']] = $row['region_count'];
        }

        static::assertSame(['north' => 3, 'south' => 3], $byRegion);
    }

    public function test_first_and_last_survive_the_filesystem_round_trip(): void
    {
        $input = [
            ['k' => 'a', 'v' => 1],
            ['k' => 'a', 'v' => 2],
            ['k' => 'a', 'v' => 3],
        ];

        $result = iterator_to_array(
            data_frame(config_builder()->groupBy(hash_group_by()->bucketsCount(3)))
                ->read(from_array($input))
                ->groupBy([ref('k')])
                ->aggregate(first(ref('v')), last(ref('v')))
                ->sortBy([ref('k')->asc()])
                ->getEachAsArray(),
        );

        static::assertSame(1, $result[0]['v_first']);
        static::assertSame(3, $result[0]['v_last']);
    }

    public function test_collect_matches_across_partition_counts(): void
    {
        $input = [
            ['k' => 'a', 'v' => 1],
            ['k' => 'b', 'v' => 4],
            ['k' => 'a', 'v' => 2],
            ['k' => 'c', 'v' => 6],
            ['k' => 'b', 'v' => 5],
            ['k' => 'a', 'v' => 3],
        ];

        $pipeline = static fn(ConfigBuilder $config): array => iterator_to_array(
            data_frame($config)
                ->read(from_array($input))
                ->groupBy([ref('k')])
                ->aggregate(collect(ref('v')), collect_unique(ref('v')))
                ->sortBy([ref('k')->asc()])
                ->getEachAsArray(),
        );

        $default = $pipeline(config_builder());
        $fewPartitions = $pipeline(config_builder()->groupBy(hash_group_by()->bucketsCount(3)));

        static::assertSame($default, $fewPartitions);

        $byKey = [];

        foreach ($default as $row) {
            $byKey[$row['k']] = [
                'collection' => $row['v_collection'],
                'collection_unique' => $row['v_collection_unique'],
            ];
        }

        static::assertSame(
            [
                'a' => ['collection' => [1, 2, 3], 'collection_unique' => [1, 2, 3]],
                'b' => ['collection' => [4, 5], 'collection_unique' => [4, 5]],
                'c' => ['collection' => [6], 'collection_unique' => [6]],
            ],
            $byKey,
        );
    }

    public function test_average_matches_across_partition_counts(): void
    {
        $input = [
            ['k' => 'a', 'v' => 2],
            ['k' => 'b', 'v' => 10],
            ['k' => 'c', 'v' => 7],
            ['k' => 'a', 'v' => 4],
            ['k' => 'b', 'v' => 20],
            ['k' => 'b', 'v' => 30],
        ];

        $pipeline = static fn(ConfigBuilder $config): array => iterator_to_array(
            data_frame($config)
                ->read(from_array($input))
                ->groupBy([ref('k')])
                ->aggregate(average(ref('v')))
                ->sortBy([ref('k')->asc()])
                ->getEachAsArray(),
        );

        $default = $pipeline(config_builder());
        $fewPartitions = $pipeline(config_builder()->groupBy(hash_group_by()->bucketsCount(3)));

        static::assertSame($default, $fewPartitions);

        $byKey = [];

        foreach ($default as $row) {
            $byKey[$row['k']] = $row['v_avg'];
        }

        static::assertSame(['a' => 3.0, 'b' => 20.0, 'c' => 7.0], $byKey);
    }

    public function test_partition_count_does_not_affect_a_realistic_dataset(): void
    {
        $raw = iterator_to_array((new FakeRandomOrdersExtractor(1000))->rawData(), false);
        $data = array_map(static fn(array $order): array => [
            'seller_id' => $order['seller_id'],
            'email' => $order['email'],
            'discount' => $order['discount'],
        ], $raw);

        // one schema per spill file: declare the source schema so a null discount carries a
        // float (nullable) definition instead of NullType, otherwise an all-null-first spill
        // batch would fix a bucket's schema to null and reject later float values
        $schema = schema(uuid_schema('seller_id'), string_schema('email'), float_schema('discount', true));

        $pipeline = static fn(ConfigBuilder $config): array => iterator_to_array(
            data_frame($config)
                ->read(from_array($data, $schema))
                ->groupBy([ref('email')])
                ->aggregate(count(ref('email')), sum(ref('discount')))
                ->sortBy([ref('email')->asc()])
                ->getEachAsArray(),
        );

        static::assertSame(
            $pipeline(config_builder()),
            $pipeline(config_builder()->groupBy(hash_group_by()->bucketsCount(3))),
        );
    }
}
