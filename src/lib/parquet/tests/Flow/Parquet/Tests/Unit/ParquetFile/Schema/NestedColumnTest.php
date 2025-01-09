<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn};
use PHPUnit\Framework\TestCase;

final class NestedColumnTest extends TestCase
{
    public function test_column_is_list() : void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        self::assertFalse($map->isList());
        self::assertTrue($list->isList());
        self::assertFalse($struct->isList());
    }

    public function test_column_is_map() : void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        self::assertTrue($map->isMap());
        self::assertFalse($list->isMap());
        self::assertFalse($struct->isMap());
    }

    public function test_column_is_struct() : void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        self::assertFalse($map->isStruct());
        self::assertFalse($list->isStruct());
        self::assertTrue($struct->isStruct());
    }

    public function test_flat_path_for_direct_root_child() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('int'),
            FlatColumn::string('string'),
            FlatColumn::boolean('bool'),
        );

        self::assertSame('int', $schema->get('int')->flatPath());
        self::assertSame('string', $schema->get('string')->flatPath());
        self::assertSame('bool', $schema->get('bool')->flatPath());
    }

    public function test_getting_flat_list_of_children() : void
    {
        $column = NestedColumn::struct('struct_nested', [
            FlatColumn::string('string'),
            NestedColumn::struct('struct_flat', [
                FlatColumn::int32('int'),
                NestedColumn::list('list_of_ints', ListElement::int32()),
                NestedColumn::map('map_of_string_int', MapKey::string(), MapValue::int32()),
            ]),
        ]);

        self::assertSame(
            [
                'struct_nested.string',
                'struct_nested.struct_flat.int',
                'struct_nested.struct_flat.list_of_ints.list.element',
                'struct_nested.struct_flat.map_of_string_int.key_value.key',
                'struct_nested.struct_flat.map_of_string_int.key_value.value',
            ],
            \array_keys($column->childrenFlat())
        );
    }

    public function test_is_map_on_a_map_column() : void
    {
        self::assertTrue(NestedColumn::map('map', MapKey::int32(), MapValue::string())->isMap());
    }

    public function test_nested_repetitions() : void
    {
        $schema = Schema::with(
            NestedColumn::struct('struct', [
                FlatColumn::int32('int32'),
                FlatColumn::string('string'),
                NestedColumn::list('list', ListElement::structure([
                    NestedColumn::map(
                        'map',
                        MapKey::string(),
                        MapValue::int32()
                    ),
                ])),
            ]),
        );

        self::assertEquals(
            [Repetition::OPTIONAL, Repetition::OPTIONAL, Repetition::REPEATED, Repetition::OPTIONAL, Repetition::OPTIONAL, Repetition::REPEATED],
            $schema->get('struct.list.list.element.map.key_value')->repetitions()->toArray()
        );
    }
}
