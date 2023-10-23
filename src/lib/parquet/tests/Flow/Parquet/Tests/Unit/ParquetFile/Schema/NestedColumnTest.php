<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
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

        $this->assertFalse($map->isList());
        $this->assertTrue($list->isList());
        $this->assertFalse($struct->isList());
    }

    public function test_column_is_map() : void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        $this->assertTrue($map->isMap());
        $this->assertFalse($list->isMap());
        $this->assertFalse($struct->isMap());
    }

    public function test_column_is_struct() : void
    {
        $map = NestedColumn::map('map', MapKey::string(), MapValue::int32());
        $list = NestedColumn::list('list', ListElement::int32());
        $struct = NestedColumn::struct('struct', [
            FlatColumn::int32('int32'),
            FlatColumn::string('string'),
        ]);

        $this->assertFalse($map->isStruct());
        $this->assertFalse($list->isStruct());
        $this->assertTrue($struct->isStruct());
    }

    public function test_flat_path_for_direct_root_child() : void
    {
        $schema = Schema::with(
            FlatColumn::int32('int'),
            FlatColumn::string('string'),
            FlatColumn::boolean('bool'),
        );

        $this->assertSame('int', $schema->get('int')->flatPath());
        $this->assertSame('string', $schema->get('string')->flatPath());
        $this->assertSame('bool', $schema->get('bool')->flatPath());
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

        $this->assertSame(
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
}
