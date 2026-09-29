<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DateTimeZone;
use Flow\ETL\Column\Php\ListColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\list_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_time_zone;

final class ListColumnTest extends TestCase
{
    public function test_reads_physical_and_logical_cells(): void
    {
        $column = ColumnMother::of(list_schema('a', type_list(type_time_zone()), nullable: true), [
            ['UTC'],
            null,
            [],
            ['UTC', 'Europe/Warsaw'],
        ]);

        static::assertInstanceOf(ListColumn::class, $column);
        static::assertSame(4, $column->count());
        static::assertSame(1, $column->nullCount());
        static::assertSame(['UTC', 'Europe/Warsaw'], $column->at(3));
        static::assertNull($column->at(1));
        static::assertSame([], $column->at(2));
        static::assertSame([['UTC'], null, [], ['UTC', 'Europe/Warsaw']], $column->physicals());
        static::assertEquals([new DateTimeZone('UTC'), new DateTimeZone('Europe/Warsaw')], $column->value(3));
        static::assertNull($column->value(1));
        static::assertEquals(
            [[new DateTimeZone('UTC')], null, [], [new DateTimeZone('UTC'), new DateTimeZone('Europe/Warsaw')]],
            $column->values(),
        );
        static::assertTrue($column->isNull(1));
        static::assertFalse($column->isNull(2));
        static::assertEquals(type_list(type_time_zone()), $column->type());
    }

    public function test_reads_cells_over_a_constant_element(): void
    {
        $column = new ListColumn(
            type_list(type_time_zone()),
            [0, 2, 2, 3],
            (new PhpBackend())->constant(definition_from_type('element', type_time_zone()), 'UTC', 3),
            [1 => true],
        );

        static::assertSame(['UTC', 'UTC'], $column->at(0));
        static::assertNull($column->at(1));
        static::assertSame(['UTC'], $column->at(2));
        static::assertEquals([new DateTimeZone('UTC'), new DateTimeZone('UTC')], $column->value(0));
        static::assertNull($column->value(1));
        static::assertEquals([new DateTimeZone('UTC')], $column->value(2));
    }

    public function test_slice_and_take_rebuild_offsets_from_zero(): void
    {
        $column = ColumnMother::of(list_schema('a', type_list(type_time_zone()), nullable: true), [
            ['UTC'],
            null,
            ['Europe/Warsaw', 'UTC'],
        ]);
        $taken = $column->take([2, 1, 0]);

        static::assertInstanceOf(ListColumn::class, $taken);
        static::assertSame([0, 2, 2, 3], $taken->offsets);
        static::assertSame([1 => true], $taken->nulls);
        static::assertSame([['Europe/Warsaw', 'UTC'], null, ['UTC']], $taken->physicals());
        static::assertSame([null, ['Europe/Warsaw', 'UTC']], $column->slice(1, 2)->physicals());
        static::assertSame(0, $column->slice(0, 0)->count());
    }
}
