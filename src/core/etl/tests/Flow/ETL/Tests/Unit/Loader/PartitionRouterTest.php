<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Tests\Context\PartitionRoutingContext;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PartitionRouterTest extends FlowTestCase
{
    public function test_a_batch_carrying_two_combinations_yields_two_groups(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('region')),
            rows(
                schema(int_schema('id'), str_schema('region')),
                row(['id' => 1, 'region' => 'eu']),
                row(['id' => 2, 'region' => 'us']),
                row(['id' => 3, 'region' => 'eu']),
            ),
        );

        static::assertSame(
            [['eu', [1, 3]], ['us', [2]]],
            array_map(static fn(array $group): array => [
                $group[0]->get('region')->value,
                $group[1]->reduceToArray(ref('id')),
            ], $groups),
        );
    }

    public function test_partition_columns_are_stripped_from_the_body_by_default(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('region')),
            rows(schema(int_schema('id'), str_schema('region')), row(['id' => 1, 'region' => 'eu'])),
        );

        static::assertSame(['id'], $groups[0][1]->schema()->references()->names());
        static::assertSame([['id' => 1]], $groups[0][1]->toArray());
    }

    public function test_write_columns_keeps_the_partition_column_in_the_body(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('region'))->writeColumns(),
            rows(schema(int_schema('id'), str_schema('region')), row(['id' => 1, 'region' => 'eu'])),
        );

        static::assertSame(['id', 'region'], $groups[0][1]->schema()->references()->names());
    }

    public function test_a_null_partition_value_becomes_the_hive_sentinel_on_the_path(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('region')),
            rows(schema(int_schema('id'), str_schema('region', nullable: true)), row(['id' => 1, 'region' => null])),
        );

        static::assertNull($groups[0][0]->get('region')->value);
        static::assertSame('region=__HIVE_DEFAULT_PARTITION__', $groups[0][0]->get('region')->segment());
    }

    public function test_partitions_keep_declaration_order_not_name_order(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('year'), ref('day'), ref('month')),
            rows(
                schema(int_schema('id'), str_schema('year'), str_schema('month'), str_schema('day')),
                row(['id' => 1, 'year' => '2024', 'month' => '03', 'day' => '01']),
            ),
        );

        static::assertSame(
            ['year', 'day', 'month'],
            array_map(static fn($partition): string => $partition->name, $groups[0][0]->toArray()),
        );
    }

    public function test_partitioning_by_every_column_leaves_nothing_to_write(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No column left to write, every column is a partition column: "region"');

        PartitionRoutingContext::route(
            Partitioning::by('region'),
            rows(schema(str_schema('region')), row(['region' => 'eu'])),
        );
    }

    public function test_partitioning_by_every_column_is_allowed_with_write_columns(): void
    {
        static::assertSame(
            [['region' => 'eu']],
            PartitionRoutingContext::route(
                Partitioning::by('region')->writeColumns(),
                rows(schema(str_schema('region')), row(['region' => 'eu'])),
            )[0][1]->toArray(),
        );
    }
}
