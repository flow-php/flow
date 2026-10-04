<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use DateTimeImmutable;
use Flow\ETL\Dataset\Statistics\Column;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnMother;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;

final class ColumnTest extends FlowTestCase
{
    public function test_collecting_column_statistics(): void
    {
        $definition = int_schema('a', true);

        $statistics = new Column($definition, ColumnMother::of($definition, [1]));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(1, $statistics->max());
        static::assertEquals(1, $statistics->min());

        $statistics->add($definition, ColumnMother::of($definition, [2]));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(2, $statistics->max());
        static::assertEquals(1, $statistics->min());

        $statistics->add($definition, ColumnMother::of($definition, [null]));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(2, $statistics->max());
        static::assertEquals(1, $statistics->min());
    }

    public function test_collecting_column_statistics_for_time_zone_entries(): void
    {
        $definition = time_zone_schema('tz', true);

        $statistics = new Column($definition, ColumnMother::of($definition, [type_time_zone()->cast('UTC')]));
        static::assertEquals(1, $statistics->distinctCount());

        $statistics->add($definition, ColumnMother::of($definition, [type_time_zone()->cast('Europe/Warsaw')]));
        static::assertEquals(2, $statistics->distinctCount());

        $statistics->add($definition, ColumnMother::of($definition, [type_time_zone()->cast('UTC')]));
        static::assertEquals(2, $statistics->distinctCount());

        $statistics->add($definition, ColumnMother::of($definition, [null]));
        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
    }

    public function test_collecting_column_statistics_for_date_entries(): void
    {
        $definition = date_schema('a', true);

        $statistics = new Column($definition, ColumnMother::of($definition, [type_date()->cast('2024-01-01')]));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->add($definition, ColumnMother::of($definition, [type_date()->cast('2024-01-05')]));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-05'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->add($definition, ColumnMother::of($definition, [null]));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-05'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->min());
    }

    public function test_collecting_column_statistics_for_datetime_entries(): void
    {
        $definition = datetime_schema('a', true);

        $statistics = new Column($definition, ColumnMother::of($definition, [type_datetime()->cast(
            '2024-01-01 00:00:01',
        )]));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->add($definition, ColumnMother::of($definition, [type_datetime()->cast('2024-01-01 01:00:00')]));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->add($definition, ColumnMother::of($definition, [null]));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());
    }

    public function test_a_column_of_many_values_with_nulls(): void
    {
        $definition = str_schema('s', true);

        $statistics = new Column($definition, ColumnMother::of($definition, ['abc', null, 'a', 'abc', null]));

        static::assertSame(2, $statistics->nullCount());
        static::assertSame(2, $statistics->distinctCount());
        static::assertSame(1, $statistics->minLength());
        static::assertSame(3, $statistics->maxLength());
    }

    public function test_a_column_of_only_nulls_counts_nulls(): void
    {
        $definition = int_schema('a', true);

        $statistics = new Column($definition, ColumnMother::of($definition, [null, null]));

        static::assertSame(2, $statistics->nullCount());
        static::assertSame(0, $statistics->distinctCount());
        static::assertNull($statistics->min());
    }

    public function test_another_column_is_ignored(): void
    {
        $statistics = new Column(int_schema('a'), ColumnMother::of(int_schema('a'), [1]));

        $statistics->add(int_schema('b'), ColumnMother::of(int_schema('b'), [5]));

        static::assertSame(1, $statistics->max());
    }

    public function test_booleans_and_floats(): void
    {
        $flags = new Column(bool_schema('b'), ColumnMother::of(bool_schema('b'), [true, false, true]));
        $amounts = new Column(float_schema('f'), ColumnMother::of(float_schema('f'), [1.5, -0.5]));

        static::assertSame(2, $flags->distinctCount());
        static::assertFalse($flags->min());
        static::assertTrue($flags->max());
        static::assertSame(-0.5, $amounts->min());
        static::assertSame(1.5, $amounts->max());
    }

    public function test_datetimes_before_the_epoch_count_distinct_seconds(): void
    {
        $definition = datetime_schema('a');

        $statistics = new Column($definition, ColumnMother::of($definition, [
            new DateTimeImmutable('1969-12-31 23:59:59.5'),
            new DateTimeImmutable('1969-12-31 23:59:59.25'),
            new DateTimeImmutable('1970-01-01 00:00:00'),
        ]));

        static::assertSame(2, $statistics->distinctCount());
        static::assertEquals(new DateTimeImmutable('1969-12-31 23:59:59.25'), $statistics->min());
        static::assertEquals(new DateTimeImmutable('1970-01-01 00:00:00'), $statistics->max());
    }

    public function test_lists_and_maps_count_elements(): void
    {
        $lists = new Column(
            list_schema('l', type_list(type_integer())),
            ColumnMother::of(list_schema('l', type_list(type_integer())), [[1, 2, 3], [], [1, 2, 3]]),
        );
        $maps = new Column(
            map_schema('m', type_map(type_string(), type_integer())),
            ColumnMother::of(map_schema('m', type_map(type_string(), type_integer())), [['a' => 1]]),
        );

        static::assertSame(2, $lists->distinctCount());
        static::assertSame(0, $lists->minElementsCount());
        static::assertSame(3, $lists->maxElementsCount());
        static::assertSame(1, $maps->maxElementsCount());
    }

    public function test_uuids_structures_and_json_count_by_value(): void
    {
        $uuids = new Column(
            uuid_schema('u'),
            ColumnMother::of(
                uuid_schema('u'),
                [
                    '00000000-0000-4000-8000-000000000001',
                    '00000000-0000-4000-8000-000000000001',
                ],
            ),
        );
        $structures = new Column(
            structure_schema('s', type_structure(['a' => type_integer()])),
            ColumnMother::of(structure_schema('s', type_structure(['a' => type_integer()])), [['a' => 1], ['a' => 2]]),
        );
        $json = new Column(json_schema('j'), ColumnMother::of(json_schema('j'), ['{"a":1}']));

        static::assertSame(1, $uuids->distinctCount());
        static::assertSame(2, $structures->distinctCount());
        static::assertSame(0, $json->distinctCount());
    }
}
