<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use PHPUnit\Framework\TestCase;

final class NestedColumnTest extends TestCase
{
    public function test_column_is_list(): void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        static::assertFalse($map->isList());
        static::assertTrue($list->isList());
        static::assertFalse($struct->isList());
    }

    public function test_column_is_map(): void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        static::assertTrue($map->isMap());
        static::assertFalse($list->isMap());
        static::assertFalse($struct->isMap());
    }

    public function test_column_is_struct(): void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        static::assertFalse($map->isStruct());
        static::assertFalse($list->isStruct());
        static::assertTrue($struct->isStruct());
    }

    public function test_flat_path_for_direct_root_child(): void
    {
        $schema = Schema::with(FlatColumn::int32('int'), FlatColumn::string('string'), FlatColumn::boolean('bool'));

        static::assertSame('int', $schema->get('int')->flatPath());
        static::assertSame('string', $schema->get('string')->flatPath());
        static::assertSame('bool', $schema->get('bool')->flatPath());
    }

    public function test_getting_flat_list_of_children(): void
    {
        $column = NestedColumn::struct('struct_nested', [
            FlatColumn::string('string'),
            NestedColumn::struct('struct_flat', [
                FlatColumn::int32('int'),
                NestedColumn::list('list_of_ints', ListElement::int32()),
                NestedColumn::map('map_of_string_int', MapKey::string(), MapValue::int32()),
            ]),
        ]);

        static::assertSame(
            [
                'struct_nested.string',
                'struct_nested.struct_flat.int',
                'struct_nested.struct_flat.list_of_ints.list.element',
                'struct_nested.struct_flat.map_of_string_int.key_value.key',
                'struct_nested.struct_flat.map_of_string_int.key_value.value',
            ],
            \array_keys($column->childrenFlat()),
        );
    }

    public function test_get_list_element_returns_inner_column(): void
    {
        $list = NestedColumn::list('list', ListElement::int32());

        $element = $list->getListElement();

        static::assertInstanceOf(FlatColumn::class, $element);
        static::assertSame('element', $element->name());
    }

    public function test_get_list_element_throws_on_non_list(): void
    {
        $struct = NestedColumn::struct('struct', [FlatColumn::int32('int')]);

        $this->expectException(InvalidArgumentException::class);
        $struct->getListElement();
    }

    public function test_get_map_key_column_returns_key(): void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());

        $key = $map->getMapKeyColumn();

        static::assertSame('key', $key->name());
    }

    public function test_get_map_key_column_throws_on_non_map(): void
    {
        $list = NestedColumn::list('list', ListElement::int32());

        $this->expectException(InvalidArgumentException::class);
        $list->getMapKeyColumn();
    }

    public function test_get_map_value_column_returns_value(): void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());

        $value = $map->getMapValueColumn();

        static::assertNotNull($value);
        static::assertSame('value', $value->name());
    }

    public function test_get_map_value_column_throws_on_non_map(): void
    {
        $struct = NestedColumn::struct('struct', [FlatColumn::int32('int')]);

        $this->expectException(InvalidArgumentException::class);
        $struct->getMapValueColumn();
    }

    public function test_is_map_on_a_map_column(): void
    {
        static::assertTrue(NestedColumn::map('map', MapKey::int32(), MapValue::string())->isMap());
    }

    public function test_nested_repetitions(): void
    {
        $schema = Schema::with(NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
            NestedColumn::list('list', ListElement::structure([
                NestedColumn::map('map', MapKey::string(), MapValue::int32()),
            ])),
        ]));

        static::assertEquals(
            [
                Repetition::OPTIONAL,
                Repetition::OPTIONAL,
                Repetition::REPEATED,
                Repetition::OPTIONAL,
                Repetition::OPTIONAL,
                Repetition::REPEATED,
            ],
            $schema->get('struct.list.list.element.map.key_value')->repetitions()->toArray(),
        );
    }
}
