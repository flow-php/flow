<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Fixtures\Enum\BackedIntEnum;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\Mother\ColumnMother;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ColumnWithTypeTest extends TestCase
{
    public function test_a_restamp_keeps_the_values(): void
    {
        $column = ColumnMother::of(int_schema('id'), [1, 2]);

        static::assertSame([1, 2], $column->withType(type_integer())->values());
    }

    public function test_a_datetime_zone_change_keeps_the_instants(): void
    {
        $column = ColumnMother::of(datetime_schema('a'), [new DateTimeImmutable('2024-01-02 03:04:05 UTC')]);
        $warsaw = $column->withType(type_datetime('Europe/Warsaw'));
        $value = type_instance_of(DateTimeImmutable::class)->assert($warsaw->value(0));

        static::assertSame($column->physicals(), $warsaw->physicals());
        static::assertInstanceOf(DateTimeImmutable::class, $value);
        static::assertSame('Europe/Warsaw', $value->getTimezone()->getName());
        static::assertSame('2024-01-02 04:04:05', $value->format('Y-m-d H:i:s'));
    }

    public function test_a_nullable_list_element_narrows_without_nulls(): void
    {
        $column = ColumnMother::of(list_schema('a', type_list(type_optional(type_integer()))), [[1, 2]]);

        static::assertSame([[1, 2]], $column->withType(type_list(type_integer()))->values());
    }

    public function test_a_nullable_list_element_with_a_null_does_not_narrow(): void
    {
        $column = ColumnMother::of(list_schema('a', type_list(type_optional(type_integer()))), [[1, null]]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('1 null values under a required type');

        $column->withType(type_list(type_integer()));
    }

    public function test_a_map_retypes_its_values(): void
    {
        $column = ColumnMother::of(map_schema('a', type_map(type_string(), type_optional(type_integer()))), [[
            'x' => 1,
        ]]);

        static::assertSame([['x' => 1]], $column->withType(type_map(type_string(), type_integer()))->values());
    }

    public function test_an_optional_structure_element_with_an_absent_value_does_not_become_required(): void
    {
        $column = ColumnMother::of(
            structure_schema('a', type_structure([
                'x' => type_integer(),
                'y' => structure_element('y', type_integer(), true),
            ])),
            [['x' => 1]],
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('1 absent values under the required element "y"');

        $column->withType(type_structure(['x' => type_integer(), 'y' => type_integer()]));
    }

    public function test_an_optional_structure_element_without_absent_values_becomes_required(): void
    {
        $column = ColumnMother::of(
            structure_schema(
                'a',
                type_structure(['x' => type_integer(), 'y' => structure_element('y', type_integer(), true)]),
                nullable: true,
            ),
            [['x' => 1, 'y' => 2], null],
        );

        static::assertSame(
            [['x' => 1, 'y' => 2], null],
            $column->withType(type_structure(['x' => type_integer(), 'y' => type_integer()]))->values(),
        );
    }

    public function test_a_renamed_structure_element_is_another_kind(): void
    {
        $column = ColumnMother::of(structure_schema('a', type_structure(['x' => type_integer()])), [['x' => 1]]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('structure{x: integer} and structure{z: integer} are different column kinds');

        $column->withType(type_structure(['z' => type_integer()]));
    }

    public function test_an_integer_does_not_become_a_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('integer and string are different column kinds');

        ColumnMother::of(int_schema('id'), [1])->withType(type_string());
    }

    public function test_an_enum_does_not_change_its_class(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('are different column kinds');

        ColumnMother::of(enum_schema('a', BackedStringEnum::class), [BackedStringEnum::one])->withType(type_enum(
            BackedIntEnum::class,
        ));
    }
}
