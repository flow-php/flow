<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\Codename;

use codename\parquet\data\DataField;
use codename\parquet\data\DataType;
use codename\parquet\data\DateTimeDataField;
use codename\parquet\data\Schema as ParquetSchema;
use Flow\ETL\Adapter\Parquet\Codename\SchemaConverter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Schema;
use PHPUnit\Framework\TestCase;

final class SchemaConverterTest extends TestCase
{
    public function test_convert_array_entry_to_parquet_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("ArrayEntry entry can't be saved in Parquet file, try convert it to ListEntry");

        (new SchemaConverter())->toParquet(new Schema(
            Schema\Definition::array('array')
        ));
    }

    public function test_convert_object_entry_to_parquet_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Flow\ETL\Row\Entry\ObjectEntry is not yet supported.");

        (new SchemaConverter())->toParquet(new Schema(
            Schema\Definition::object('object')
        ));
    }

    public function test_convert_enum_entry_to_parquet_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Flow\ETL\Row\Entry\EnumEntry is not yet supported.");

        (new SchemaConverter())->toParquet(new Schema(
            Schema\Definition::enum('enum')
        ));
    }

    public function test_convert_etl_entries_to_parquet_fields() : void
    {
        $this->assertEquals(
            new ParquetSchema([
                new DataField('integer', DataType::Int32, true, false),
                new DataField('boolean', DataType::Boolean, true, false),
                new DataField('string', DataType::String, true, false),
                new DataField('float', DataType::Float, true, false),
                new DateTimeDataField('datetime', DataType::DateTimeOffset, true, false),
                new DataField('json', DataType::String, true, false),
            ]),
            (new SchemaConverter())->toParquet(new Schema(
                Schema\Definition::integer('integer'),
                Schema\Definition::boolean('boolean'),
                Schema\Definition::string('string'),
                Schema\Definition::float('float'),
                Schema\Definition::dateTime('datetime'),
                Schema\Definition::json('json'),
            ))
        );
    }
}
