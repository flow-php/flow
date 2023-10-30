<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;

final class SchemaReadingTest extends ParquetIntegrationTestCase
{
    public function test_reading_lists_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());

        $schema = new Schema(
            NestedColumn::create('schema', [
                NestedColumn::list('list', ListElement::int32()),
                NestedColumn::list('list_nullable', ListElement::int32()),
                NestedColumn::list('list_mixed_types', ListElement::structure([
                    FlatColumn::int32('int'),
                    FlatColumn::string('string'),
                    FlatColumn::boolean('bool'),
                ])),
                NestedColumn::list('list_nested', ListElement::list(ListElement::list(ListElement::int32()))),
            ])
        );

        $this->assertSame(
            ($reader->read(__DIR__ . '/../../Fixtures/lists.parquet'))->metadata()->schema()->toDDL(),
            $schema->toDDL(),
        );
    }

    public function test_reading_maps_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());

        $schema = Schema::with(
            NestedColumn::map('map', MapKey::string(), MapValue::int32()),
            NestedColumn::map('map_nullable', MapKey::string(), MapValue::int32()),
            NestedColumn::map('map_of_maps', MapKey::string(), MapValue::map(MapKey::string(), MapValue::int32())),
            NestedColumn::map('map_of_lists', MapKey::string(), MapValue::list(ListElement::int32())),
            NestedColumn::map('map_of_complex_lists', MapKey::string(), MapValue::list(ListElement::structure([
                FlatColumn::int32('int'),
                FlatColumn::string('string'),
                FlatColumn::boolean('bool'),
            ]))),
            NestedColumn::map('map_of_list_of_map_of_lists', MapKey::string(), MapValue::list(ListElement::map(MapKey::string(), MapValue::list(ListElement::int32())))),
            NestedColumn::map('map_of_structs', MapKey::string(), MapValue::structure([
                FlatColumn::int32('int_field'),
                FlatColumn::string('string_field'),
            ])),
            NestedColumn::map('map_of_struct_of_structs', MapKey::string(), MapValue::structure([
                NestedColumn::struct('struct', [
                    NestedColumn::struct('nested_struct', [
                        FlatColumn::int32('int'),
                        FlatColumn::string('string'),
                    ]),
                ]),
            ])),
        );

        $this->assertSame(
            ($reader->read(__DIR__ . '/../../Fixtures/maps.parquet'))->metadata()->schema()->toDDL(),
            $schema->toDDL(),
        );
    }

    public function test_reading_primitives_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());

        $schema = Schema::with(
            FlatColumn::int32('int32'),
            FlatColumn::int32('int32_nullable'),
            FlatColumn::int64('int64'),
            FlatColumn::int64('int64_nullable'),
            FlatColumn::boolean('bool'),
            FlatColumn::boolean('bool_nullable'),
            FlatColumn::string('string'),
            FlatColumn::string('string_nullable'),
            FlatColumn::json('json'),
            FlatColumn::json('json_nullable'),
            FlatColumn::date('date'),
            FlatColumn::date('date_nullable'),
            FlatColumn::dateTime('timestamp'),
            FlatColumn::dateTime('timestamp_nullable'),
            FlatColumn::time('time'),
            FlatColumn::time('time_nullable'),
            FlatColumn::uuid('uuid'),
            FlatColumn::uuid('uuid_nullable'),
            FlatColumn::enum('enum'),
            FlatColumn::enum('enum_nullable'),
            FlatColumn::float('float'),
            FlatColumn::float('float_nullable'),
            FlatColumn::double('double'),
            FlatColumn::double('double_nullable'),
            FlatColumn::decimal('decimal'),
            FlatColumn::decimal('decimal_nullable'),
        );

        $this->assertSame(
            ($reader->read(__DIR__ . '/../../Fixtures/primitives.parquet'))->metadata()->schema()->toDDL(),
            $schema->toDDL()
        );
    }

    public function test_reading_structs_schema_ddl() : void
    {
        $reader = new Reader(logger: $this->getLogger());

        $schema = Schema::with(
            NestedColumn::struct('struct_flat', [
                FlatColumn::string('string'),
                FlatColumn::string('string_nullable'),
                FlatColumn::int32('int'),
                FlatColumn::int32('int_nullable'),
                FlatColumn::boolean('bool'),
                FlatColumn::boolean('bool_nullable'),
                NestedColumn::list('list_of_ints', ListElement::int32()),
                NestedColumn::list('list_of_strings', ListElement::string()),
                NestedColumn::map('map_of_string_int', MapKey::string(), MapValue::int32()),
                NestedColumn::map('map_of_int_int', MapKey::int32(), MapValue::int32()),
            ]),
            NestedColumn::struct('struct_flat_nullable', [
                FlatColumn::string('string'),
                FlatColumn::string('string_nullable'),
                FlatColumn::int32('int'),
                FlatColumn::int32('int_nullable'),
                FlatColumn::boolean('bool'),
                FlatColumn::boolean('bool_nullable'),
                NestedColumn::list('list_of_ints', ListElement::int32()),
                NestedColumn::list('list_of_strings', ListElement::string()),
                NestedColumn::map('map_of_string_int', MapKey::string(), MapValue::int32()),
                NestedColumn::map('map_of_int_int', MapKey::int32(), MapValue::int32()),
            ]),
            NestedColumn::struct('struct_nested', [
                FlatColumn::string('string'),
                NestedColumn::struct('struct_flat', [
                    FlatColumn::int32('int'),
                    NestedColumn::list('list_of_ints', ListElement::int32()),
                    NestedColumn::map('map_of_string_int', MapKey::string(), MapValue::int32()),
                ]),
            ]),
            NestedColumn::struct('struct_nested_with_list_of_lists', [
                FlatColumn::string('string'),
                NestedColumn::struct('struct', [
                    FlatColumn::int32('int'),
                    NestedColumn::list('list_of_list_of_ints', ListElement::list(ListElement::int32())),
                ]),
            ]),
            NestedColumn::struct('struct_nested_with_list_of_maps', [
                FlatColumn::string('string'),
                NestedColumn::struct('struct', [
                    FlatColumn::int32('int'),
                    NestedColumn::list('list_of_map_of_string_int', ListElement::map(MapKey::string(), MapValue::int32())),
                ]),
            ]),
            NestedColumn::struct('struct_nested_with_map_of_list_of_ints', [
                FlatColumn::string('string'),
                NestedColumn::struct('struct', [
                    FlatColumn::int32('int'),
                    NestedColumn::map('map_of_int_list_of_string', MapKey::int32(), MapValue::list(ListElement::string())),
                ]),
            ]),
            NestedColumn::struct('struct_nested_with_map_of_string_map_of_string_string', [
                FlatColumn::string('string'),
                NestedColumn::struct('struct', [
                    FlatColumn::int32('int'),
                    NestedColumn::map('map_of_string_map_of_string_string', MapKey::string(), MapValue::map(MapKey::string(), MapValue::string())),
                ]),
            ]),
            NestedColumn::struct('struct_with_list_and_map_of_structs', [
                FlatColumn::string('string'),
                NestedColumn::struct('struct', [
                    FlatColumn::int32('int'),
                    NestedColumn::list('list_of_structs', ListElement::structure([
                        FlatColumn::int32('int'),
                        NestedColumn::list('list', ListElement::int32()),
                    ])),
                    NestedColumn::map('map_of_string_structs', MapKey::string(), MapValue::structure([
                        FlatColumn::int32('int'),
                        NestedColumn::list('list', ListElement::int32()),
                    ])),
                ]),
            ]),
            NestedColumn::struct('struct_deeply_nested', [
                NestedColumn::struct('struct_0', [
                    FlatColumn::int32('int'),
                    NestedColumn::struct('struct_1', [
                        FlatColumn::string('string'),
                        NestedColumn::struct('struct_2', [
                            FlatColumn::boolean('bool'),
                            NestedColumn::struct('struct_3', [
                                FlatColumn::float('float'),
                                NestedColumn::struct('struct_4', [
                                    FlatColumn::string('string'),
                                    FlatColumn::json('json'),
                                ]),
                            ]),
                        ]),
                    ]),
                ]),
            ])
        );

        $this->assertSame(
            ($reader->read(__DIR__ . '/../../Fixtures/structs.parquet'))->metadata()->schema()->toDDL(),
            $schema->toDDL()
        );
    }
}
