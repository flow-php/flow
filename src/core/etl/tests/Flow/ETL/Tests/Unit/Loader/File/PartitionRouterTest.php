<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader\File;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Tests\Context\PartitionRoutingContext;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PartitionRouterTest extends FlowTestCase
{
    public function test_a_batch_carrying_two_combinations_yields_two_groups(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('region')),
            array_to_rows(
                [['id' => 1, 'region' => 'eu'], ['id' => 2, 'region' => 'us'], ['id' => 3, 'region' => 'eu']],
                schema(int_schema('id'), str_schema('region')),
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
            array_to_rows([['id' => 1, 'region' => 'eu']], schema(int_schema('id'), str_schema('region'))),
        );

        static::assertSame(['id'], $groups[0][1]->schema()->references()->names());
        static::assertSame([['id' => 1]], $groups[0][1]->toArray());
    }

    public function test_write_columns_keeps_the_partition_column_in_the_body(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('region'))->writeColumns(),
            array_to_rows([['id' => 1, 'region' => 'eu']], schema(int_schema('id'), str_schema('region'))),
        );

        static::assertSame(['id', 'region'], $groups[0][1]->schema()->references()->names());
    }

    public function test_a_null_partition_value_becomes_the_hive_sentinel_on_the_path(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('region')),
            array_to_rows(
                [['id' => 1, 'region' => null]],
                schema(int_schema('id'), str_schema('region', nullable: true)),
            ),
        );

        static::assertNull($groups[0][0]->get('region')->value);
        static::assertSame('region=__HIVE_DEFAULT_PARTITION__', $groups[0][0]->get('region')->segment());
    }

    public function test_partitions_keep_declaration_order_not_name_order(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('year'), ref('day'), ref('month')),
            array_to_rows(
                [['id' => 1, 'year' => '2024', 'month' => '03', 'day' => '01']],
                schema(int_schema('id'), str_schema('year'), str_schema('month'), str_schema('day')),
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
            array_to_rows([['region' => 'eu']], schema(str_schema('region'))),
        );
    }

    public function test_partitioning_by_every_column_is_allowed_with_write_columns(): void
    {
        static::assertSame(
            [['region' => 'eu']],
            PartitionRoutingContext::route(
                Partitioning::by('region')->writeColumns(),
                array_to_rows([['region' => 'eu']], schema(str_schema('region'))),
            )[0][1]->toArray(),
        );
    }

    public function test_equal_datetime_instants_route_to_one_partition_and_keep_the_path_name(): void
    {
        $groups = PartitionRoutingContext::route(
            Partitioning::by(ref('at')),
            array_to_rows(
                [
                    ['id' => 1, 'at' => new DateTimeImmutable('2026-01-02 10:00:00 UTC')],
                    ['id' => 2, 'at' => new DateTimeImmutable('2026-01-02 10:00:00 UTC')],
                ],
                schema(int_schema('id'), datetime_schema('at')),
            ),
        );

        static::assertCount(1, $groups);
        static::assertSame([1, 2], $groups[0][1]->reduceToArray(ref('id')));
        static::assertSame('2026-01-02', $groups[0][0]->get('at')->value);
    }
}
