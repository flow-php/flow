<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\ValueConverter;

use Flow\ETL\Adapter\Parquet\ValueConverter\ElementsValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\JsonValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\StructValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\UuidValueConverter;
use Flow\ETL\Adapter\Parquet\ValueConverter\ValueConverters;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

final class ValueConvertersTest extends FlowTestCase
{
    public function test_deeply_nested_struct_with_json_leaf(): void
    {
        static::assertInstanceOf(
            StructValueConverter::class,
            ValueConverters::for(NestedColumn::struct('outer', [
                NestedColumn::struct('inner', [FlatColumn::json('deep')]),
            ])),
        );
    }

    public function test_flat_json_column(): void
    {
        static::assertInstanceOf(JsonValueConverter::class, ValueConverters::for(FlatColumn::json('payload')));
    }

    public function test_flat_scalar_columns_need_no_converter(): void
    {
        static::assertNull(ValueConverters::for(FlatColumn::int64('id')));
        static::assertNull(ValueConverters::for(FlatColumn::string('name')));
        static::assertNull(ValueConverters::for(FlatColumn::dateTime('created_at')));
    }

    public function test_flat_uuid_column(): void
    {
        static::assertInstanceOf(UuidValueConverter::class, ValueConverters::for(FlatColumn::uuid('id')));
    }

    public function test_list_of_scalars_needs_no_converter(): void
    {
        static::assertNull(ValueConverters::for(NestedColumn::list('items', ListElement::string())));
    }

    public function test_list_of_uuid(): void
    {
        static::assertInstanceOf(
            ElementsValueConverter::class,
            ValueConverters::for(NestedColumn::list('ids', ListElement::uuid())),
        );
    }

    public function test_map_with_scalar_values_needs_no_converter(): void
    {
        static::assertNull(ValueConverters::for(NestedColumn::map('meta', MapKey::string(), MapValue::string())));
    }

    public function test_map_with_uuid_values(): void
    {
        static::assertInstanceOf(
            ElementsValueConverter::class,
            ValueConverters::for(NestedColumn::map('ids', MapKey::string(), MapValue::uuid())),
        );
    }

    public function test_struct_with_json_leaf(): void
    {
        static::assertInstanceOf(
            StructValueConverter::class,
            ValueConverters::for(NestedColumn::struct('body', [FlatColumn::json('data'), FlatColumn::int64('n')])),
        );
    }

    public function test_struct_without_convertible_leaves_needs_no_converter(): void
    {
        static::assertNull(ValueConverters::for(NestedColumn::struct('body', [
            FlatColumn::string('name'),
            FlatColumn::int64('n'),
        ])));
    }
}
