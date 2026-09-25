<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Definition;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Schema\Definition\ZoneAlignment;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ZoneAlignmentTest extends FlowTestCase
{
    public function test_align_hops_a_foreign_zone(): void
    {
        static::assertSame(
            '2026-01-01 23:04:05 Europe/Warsaw',
            type_datetime()
                ->assert((new ZoneAlignment())->align(
                    datetime_schema('at', zone: 'Europe/Warsaw'),
                    new DateTimeImmutable('2026-01-02 03:04:05+05:00'),
                ))
                ->format('Y-m-d H:i:s e'),
        );
    }

    public function test_align_keeps_null(): void
    {
        static::assertNull((new ZoneAlignment())->align(datetime_schema('at', nullable: true), null));
    }

    public function test_align_recasts_a_nested_list(): void
    {
        static::assertSame(
            '2026-01-02 04:04:05 Europe/Warsaw',
            type_list(type_datetime())
                ->assert((new ZoneAlignment())->align(
                    list_schema('l', type_list(type_datetime('Europe/Warsaw'))),
                    [new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('UTC'))],
                ))[0]->format('Y-m-d H:i:s e'),
        );
    }

    public function test_align_returns_the_same_object_in_zone(): void
    {
        $value = new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('Europe/Warsaw'));

        static::assertSame($value, (new ZoneAlignment())->align(datetime_schema('at', zone: 'Europe/Warsaw'), $value));
    }

    public function test_columns_find_top_level_and_nested_datetimes(): void
    {
        static::assertSame(
            ['at', 'optional', 'list', 'map', 'structure'],
            array_keys((new ZoneAlignment())->columns([
                'at' => datetime_schema('at'),
                'optional' => list_schema('optional', type_list(type_optional(type_datetime()))),
                'list' => list_schema('list', type_list(type_datetime())),
                'map' => map_schema('map', type_map(type_string(), type_datetime())),
                'structure' => structure_schema('structure', type_structure(['at' => type_datetime()])),
                'integers' => list_schema('integers', type_list(type_integer())),
                'on' => date_schema('on'),
            ])),
        );
        static::assertTrue((new ZoneAlignment())->carriesDateTime(type_optional(type_datetime())));
    }

    public function test_columns_of_a_scalar_schema_are_empty(): void
    {
        static::assertSame(
            [],
            (new ZoneAlignment())->columns([
                'id' => int_schema('id'),
                'name' => string_schema('name'),
            ]),
        );
    }

    public function test_in_zone_needs_a_datetime_column_in_the_same_zone(): void
    {
        $value = new DateTimeImmutable('2026-01-02 03:04:05', new DateTimeZone('Europe/Warsaw'));

        static::assertTrue((new ZoneAlignment())->inZone(datetime_schema('at', zone: 'Europe/Warsaw'), $value));
        static::assertFalse((new ZoneAlignment())->inZone(datetime_schema('at'), $value));
        static::assertFalse((new ZoneAlignment())->inZone(
            list_schema('at', type_list(type_datetime('Europe/Warsaw'))),
            $value,
        ));
        static::assertFalse((new ZoneAlignment())->inZone(datetime_schema('at', zone: 'Europe/Warsaw'), '2026-01-02'));
    }
}
