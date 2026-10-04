<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DateTimeZone;
use Flow\ETL\Column\Php\MapColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\map_schema;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;

final class MapColumnTest extends TestCase
{
    public function test_reads_physical_and_logical_cells(): void
    {
        $column = ColumnMother::of(map_schema('a', type_map(type_string(), type_time_zone()), nullable: true), [
            ['x' => 'UTC'],
            null,
            [],
            ['y' => 'UTC', 'z' => 'Europe/Warsaw'],
        ]);

        static::assertInstanceOf(MapColumn::class, $column);
        static::assertSame(4, $column->count());
        static::assertSame(1, $column->nullCount());
        static::assertSame(['y' => 'UTC', 'z' => 'Europe/Warsaw'], $column->at(3));
        static::assertNull($column->at(1));
        static::assertSame([], $column->at(2));
        static::assertSame([['x' => 'UTC'], null, [], ['y' => 'UTC', 'z' => 'Europe/Warsaw']], $column->physicals());
        static::assertEquals(
            ['y' => new DateTimeZone('UTC'), 'z' => new DateTimeZone('Europe/Warsaw')],
            $column->value(3),
        );
        static::assertNull($column->value(1));
        static::assertEquals(
            [
                ['x' => new DateTimeZone('UTC')],
                null,
                [],
                ['y' => new DateTimeZone('UTC'), 'z' => new DateTimeZone('Europe/Warsaw')],
            ],
            $column->values(),
        );
        static::assertTrue($column->isNull(1));
        static::assertEquals(type_map(type_string(), type_time_zone()), $column->type());
    }

    public function test_reads_cells_over_constant_keys_and_values(): void
    {
        $backend = new PhpBackend();
        $keys = $backend->builder(definition_from_type('key', type_string()));
        $keys->appendMany(['x', 'y']);
        $column = new MapColumn(
            type_map(type_string(), type_time_zone()),
            [0, 1, 1, 2],
            $keys->finish(),
            $backend->constant(definition_from_type('value', type_time_zone()), 'UTC', 2),
            [1 => true],
        );

        static::assertSame(['x' => 'UTC'], $column->at(0));
        static::assertNull($column->at(1));
        static::assertSame(['y' => 'UTC'], $column->at(2));
        static::assertEquals(['x' => new DateTimeZone('UTC')], $column->value(0));
        static::assertNull($column->value(1));
        static::assertEquals(['y' => new DateTimeZone('UTC')], $column->value(2));
    }

    public function test_slice_and_take_rebuild_offsets_from_zero(): void
    {
        $column = ColumnMother::of(map_schema('a', type_map(type_string(), type_string()), nullable: true), [
            ['a' => 'x'],
            null,
            ['b' => 'y', 'c' => 'z'],
        ]);
        $taken = $column->take([2, 1, 0]);

        static::assertInstanceOf(MapColumn::class, $taken);
        static::assertSame([0, 2, 2, 3], $taken->offsets);
        static::assertSame([['b' => 'y', 'c' => 'z'], null, ['a' => 'x']], $taken->values());
        static::assertSame([null, ['b' => 'y', 'c' => 'z']], $column->slice(1, 2)->values());
        static::assertSame(0, $column->slice(0, 0)->count());
    }
}
