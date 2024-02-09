<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit;

use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Adapter\Parquet\SchemaConverter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\Row\Schema;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use PHPUnit\Framework\TestCase;

final class FlowToParquetSchemaTest extends TestCase
{
    public function test_convert_array_entry_to_parquet_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Flow\ETL\PHP\Type\Native\ArrayType is not supported.");

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
                FlatColumn::json('json'),
                NestedColumn::list('list', ParquetSchema\ListElement::string()),
                NestedColumn::list('list_of_structs', ParquetSchema\ListElement::structure(
                    [
                        FlatColumn::int64('integer'),
                        FlatColumn::boolean('boolean'),
                    ]
                )),
                NestedColumn::struct('structure', [FlatColumn::string('a')]),
                NestedColumn::map('map', ParquetSchema\MapKey::string(), ParquetSchema\MapValue::int64()),
                FlatColumn::time('time')
            ),
            (new SchemaConverter())->toParquet(new Schema(
                Schema\Definition::integer('integer'),
                Schema\Definition::boolean('boolean'),
                Schema\Definition::string('string'),
                Schema\Definition::float('float'),
                Schema\Definition::dateTime('datetime'),
                Schema\Definition::json('json'),
                Schema\Definition::list('list', new ListType(ListElement::string())),
                Schema\Definition::list('list_of_structs', new ListType(ListElement::structure(
                    new StructureType([
                        new StructureElement('integer', type_int()),
                        new StructureElement('boolean', type_boolean()),
                    ]),
                ))),
                Schema\Definition::structure('structure', new StructureType([new StructureElement('a', type_string())])),
                Schema\Definition::map('map', new MapType(MapKey::string(), MapValue::integer())),
                Schema\Definition::object('time', type_object(\DateInterval::class, false))
            ))
        );
    }

    public function test_convert_object_entry_to_parquet_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("stdClass can't be converted to any parquet columns.");

        (new SchemaConverter())->toParquet(new Schema(
            Schema\Definition::object('object', type_object(\stdClass::class, false))
        ));
    }
}
