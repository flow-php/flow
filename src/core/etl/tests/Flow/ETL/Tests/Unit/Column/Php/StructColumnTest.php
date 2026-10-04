<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DateTimeZone;
use Flow\ETL\Column\Php\StructColumn;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time_zone;

final class StructColumnTest extends TestCase
{
    public function test_reads_cells_with_the_presence_rule(): void
    {
        $type = type_structure([
            'id' => type_integer(),
            'zone' => structure_element('zone', type_optional(type_time_zone()), optional: true),
            'tag' => type_optional(type_integer()),
        ]);
        $column = ColumnMother::of(structure_schema('a', $type, nullable: true), [
            ['id' => 1, 'zone' => 'UTC', 'tag' => 2],
            null,
            ['id' => 3, 'tag' => null],
        ]);

        static::assertInstanceOf(StructColumn::class, $column);
        static::assertSame(3, $column->count());
        static::assertSame(1, $column->nullCount());
        static::assertTrue($column->isNull(1));
        static::assertSame(['id' => 1, 'zone' => 'UTC', 'tag' => 2], $column->at(0));
        static::assertNull($column->at(1));
        static::assertSame(['id' => 3, 'tag' => null], $column->at(2));
        static::assertSame(
            [['id' => 1, 'zone' => 'UTC', 'tag' => 2], null, ['id' => 3, 'tag' => null]],
            $column->physicals(),
        );
        static::assertEquals(['id' => 1, 'zone' => new DateTimeZone('UTC'), 'tag' => 2], $column->value(0));
        static::assertNull($column->value(1));
        static::assertSame(['id' => 3, 'tag' => null], $column->value(2));
        static::assertEquals(
            [['id' => 1, 'zone' => new DateTimeZone('UTC'), 'tag' => 2], null, ['id' => 3, 'tag' => null]],
            $column->values(),
        );
        static::assertSame($type, $column->structure());
        static::assertSame($type, $column->type());
    }

    public function test_slice_and_take_rekey_nulls(): void
    {
        $column = ColumnMother::of(structure_schema('a', type_structure(['id' => type_integer()]), nullable: true), [
            ['id' => 1],
            null,
            ['id' => 3],
        ]);
        $taken = $column->take([1, 2]);

        static::assertInstanceOf(StructColumn::class, $taken);
        static::assertSame([0 => true], $taken->nulls);
        static::assertSame([null, ['id' => 3]], $taken->values());
        static::assertSame([null, ['id' => 3]], $column->slice(1, 2)->values());
        static::assertSame(0, $column->slice(0, 0)->count());
    }
}
