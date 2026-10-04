<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_datetime;

final class MaxTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('int')], max(ref('int'))->references());
    }

    public function test_aggregation_max_from_numeric_values(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => '10']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '20']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '55']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '25']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(
            array_to_rows([[
                'not_int' => null,
            ]], schema(str_schema('not_int', nullable: true))),
            [0],
            flow_context(),
        );

        static::assertSame(55.0, $aggregator->value());
    }

    public function test_aggregation_max_including_null_value(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => 10]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 20]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 30]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(
            array_to_rows([[
                'int' => null,
            ]], schema(int_schema('int', nullable: true))),
            [0],
            flow_context(),
        );

        static::assertSame(30.0, $aggregator->value());
    }

    public function test_aggregation_max_with_datetime_values(): void
    {
        $aggregator = max(ref('datetime'));

        $aggregator->aggregate(
            array_to_rows([[
                'datetime' => type_datetime()->cast('2021-01-01 00:00:00'),
            ]], schema(datetime_schema('datetime'))),
            [0],
            flow_context(),
        );
        $aggregator->aggregate(
            array_to_rows([[
                'datetime' => type_datetime()->cast('2021-01-02 00:00:00'),
            ]], schema(datetime_schema('datetime'))),
            [0],
            flow_context(),
        );
        $aggregator->aggregate(
            array_to_rows([[
                'datetime' => type_datetime()->cast('2021-01-03 00:00:00'),
            ]], schema(datetime_schema('datetime'))),
            [0],
            flow_context(),
        );
        $aggregator->aggregate(
            array_to_rows([[
                'datetime' => type_datetime()->cast('2021-01-04 00:00:00'),
            ]], schema(datetime_schema('datetime'))),
            [0],
            flow_context(),
        );

        static::assertEquals(new DateTimeImmutable('2021-01-04 00:00:00'), $aggregator->value());
    }

    public function test_aggregation_max_with_float_result(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => 10]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 20]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 30.5]], schema(float_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 25]], schema(int_schema('int'))), [0], flow_context());

        static::assertSame(30.5, $aggregator->value());
    }

    public function test_aggregation_max_with_integer_result(): void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => 10]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 20]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 30]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 40]], schema(int_schema('int'))), [0], flow_context());

        static::assertSame(40.0, $aggregator->value());
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = max(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_max_of_nothing_is_null(): void
    {
        static::assertNull(max(ref('int'))->value());
    }

    public function test_max_over_an_int_column_declares_optional_integer(): void
    {
        static::assertSame(
            '?integer',
            (new ReferenceResolver())
                ->resolve(max(ref('int')), schema(int_schema('int')))
                ->returns()
                ->toString(),
        );
    }

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = max(ref('v'));
        $aggregator->aggregate(
            array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
                0.5,
                1.25,
                2.0,
                3.0,
            ]), schema(float_schema('v'))),
            [0, 2],
            flow_context(),
        );

        static::assertSame(2.0, $aggregator->value());
    }

    public function test_merge_of_two_halves_equals_one_pass(): void
    {
        $rows = array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
            0.5,
            1.25,
            2.0,
            3.0,
        ]), schema(float_schema('v')));
        $onePass = max(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context());
        $left = max(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context());
        $right = max(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context());

        $left->merge($right, flow_context());

        static::assertSame($onePass->value(), $left->value());
    }

    public function test_merge_refuses_another_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        max(ref('v'))->merge(min(ref('v')), flow_context());
    }

    public function test_merge_into_an_empty_half_takes_the_other_value(): void
    {
        $right = max(ref('v'));
        $right->aggregate(array_to_rows([['v' => 1.5]], schema(float_schema('v'))), [0], flow_context());
        $left = max(ref('v'));
        $left->merge(max(ref('v')), flow_context());
        $left->merge($right, flow_context());

        static::assertSame(1.5, $left->value());
    }
}
