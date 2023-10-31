<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Schema;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
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

    public function test_convert_etl_entries_to_parquet_fields() : void
    {
        $this->assertEquals(
            ParquetSchema::with(
                FlatColumn::int64('integer'),
                FlatColumn::boolean('boolean'),
                FlatColumn::string('string'),
                FlatColumn::float('float'),
                FlatColumn::dateTime('datetime'),
                FlatColumn::json('json')
            ),
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

    public function test_convert_object_entry_to_parquet_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Flow\ETL\Row\Entry\ObjectEntry is not supported.");

        (new SchemaConverter())->toParquet(new Schema(
            Schema\Definition::object('object')
        ));
    }
}
