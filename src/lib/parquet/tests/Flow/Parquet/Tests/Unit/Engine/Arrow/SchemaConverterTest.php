<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine\Arrow;

use Flow\Parquet\Engine\Arrow\SchemaConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, MapKey, MapValue, NestedColumn, Repetition};
use PHPUnit\Framework\TestCase;

final class SchemaConverterTest extends TestCase
{
    public function test_boolean_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::boolean('flag'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('flag', $result[0]['name']);
        self::assertSame('BOOLEAN', $result[0]['type']);
        self::assertTrue($result[0]['optional']);
    }

    public function test_date_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::date('created_at'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('DATE', $result[0]['type']);
    }

    public function test_datetime_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::dateTime('updated_at'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('TIMESTAMP', $result[0]['type']);
    }

    public function test_decimal_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::decimal('price', 10, 2));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('DECIMAL', $result[0]['type']);
        self::assertSame(10, $result[0]['precision']);
        self::assertSame(2, $result[0]['scale']);
    }

    public function test_double_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::double('amount'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('DOUBLE', $result[0]['type']);
    }

    public function test_enum_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::enum('status'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('STRING', $result[0]['type']);
    }

    public function test_fixed_size_byte_array_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::fixedSizeByteArray('hash', 32));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('FIXED_SIZE_BINARY', $result[0]['type']);
        self::assertSame(32, $result[0]['length']);
    }

    public function test_float_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::float('price'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('FLOAT', $result[0]['type']);
    }

    public function test_full_schema_to_extension() : void
    {
        $schema = Schema::with(
            FlatColumn::int64('id', Repetition::REQUIRED),
            FlatColumn::string('name'),
            FlatColumn::date('created_at'),
            NestedColumn::list('tags', ListElement::string()),
            NestedColumn::struct('address', [
                FlatColumn::string('street'),
                FlatColumn::int32('zip'),
            ]),
        );

        $result = SchemaConverter::toExtension($schema);

        self::assertCount(5, $result);
        self::assertSame('INT64', $result[0]['type']);
        self::assertFalse($result[0]['optional']);
        self::assertSame('STRING', $result[1]['type']);
        self::assertSame('DATE', $result[2]['type']);
        self::assertSame('LIST', $result[3]['type']);
        self::assertSame('STRUCT', $result[4]['type']);
    }

    public function test_int32_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::int32('count'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('INT32', $result[0]['type']);
    }

    public function test_int64_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::int64('big_count'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('INT64', $result[0]['type']);
    }

    public function test_json_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::json('metadata'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('JSON', $result[0]['type']);
    }

    public function test_list_column_to_extension() : void
    {
        $schema = Schema::with(
            NestedColumn::list('tags', ListElement::string())
        );
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('tags', $result[0]['name']);
        self::assertSame('LIST', $result[0]['type']);
        self::assertCount(1, $result[0]['children']);
        self::assertSame('STRING', $result[0]['children'][0]['type']);
    }

    public function test_map_column_to_extension() : void
    {
        $schema = Schema::with(
            NestedColumn::map('attributes', MapKey::string(), MapValue::int32())
        );
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('attributes', $result[0]['name']);
        self::assertSame('MAP', $result[0]['type']);
        self::assertCount(2, $result[0]['children']);
        self::assertSame('STRING', $result[0]['children'][0]['type']);
        self::assertSame('INT32', $result[0]['children'][1]['type']);
    }

    public function test_optional_column_sets_optional_true() : void
    {
        $schema = Schema::with(FlatColumn::string('name', Repetition::OPTIONAL));
        $result = SchemaConverter::toExtension($schema);

        self::assertTrue($result[0]['optional']);
    }

    public function test_required_column_sets_optional_false() : void
    {
        $schema = Schema::with(FlatColumn::string('name', Repetition::REQUIRED));
        $result = SchemaConverter::toExtension($schema);

        self::assertFalse($result[0]['optional']);
    }

    public function test_string_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::string('name'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('STRING', $result[0]['type']);
    }

    public function test_struct_column_to_extension() : void
    {
        $schema = Schema::with(
            NestedColumn::struct('address', [
                FlatColumn::string('street'),
                FlatColumn::string('city'),
            ])
        );
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('address', $result[0]['name']);
        self::assertSame('STRUCT', $result[0]['type']);
        self::assertCount(2, $result[0]['children']);
        self::assertSame('street', $result[0]['children'][0]['name']);
        self::assertSame('STRING', $result[0]['children'][0]['type']);
        self::assertSame('city', $result[0]['children'][1]['name']);
    }

    public function test_time_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::time('start_time'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('TIME', $result[0]['type']);
    }

    public function test_uuid_column_to_extension() : void
    {
        $schema = Schema::with(FlatColumn::uuid('id'));
        $result = SchemaConverter::toExtension($schema);

        self::assertSame('UUID', $result[0]['type']);
        self::assertArrayNotHasKey('length', $result[0]);
    }
}
