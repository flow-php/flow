<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\Tests\Unit;

use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Adapter\Avro\FlixTech\SchemaConverter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\Row\Schema;
use PHPUnit\Framework\TestCase;

final class SchemaConverterTest extends TestCase
{
    public function test_convert_etl_entries_to_avro_json() : void
    {
        $this->assertSame(
            <<<'AVRO_JSON'
{"name":"row","type":"record","fields":[{"name":"integer","type":"int"},{"name":"boolean","type":"boolean"},{"name":"string","type":"string"},{"name":"float","type":"float"},{"name":"datetime","type":"long","logicalType":"timestamp-micros"},{"name":"json","type":"string"},{"name":"list","type":{"type":"array","items":"string"}},{"name":"structure","type":{"name":"Structure","type":"record","fields":[{"name":"a","type":"string"}]}},{"name":"map","type":{"type":"map","values":"int"}}]}
AVRO_JSON
            ,
            (new SchemaConverter())->toAvroJsonSchema(new Schema(
                Schema\Definition::integer('integer'),
                Schema\Definition::boolean('boolean'),
                Schema\Definition::string('string'),
                Schema\Definition::float('float'),
                Schema\Definition::dateTime('datetime'),
                Schema\Definition::json('json'),
                Schema\Definition::list('list', new ListType(ListElement::string())),
                Schema\Definition::structure('structure', new StructureType([new StructureElement('a', type_string())])),
                Schema\Definition::map('map', new MapType(MapKey::string(), MapValue::integer()))
            ))
        );
    }

    public function test_convert_object_entry_to_avro_array() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Flow\ETL\PHP\Type\Native\ObjectType is not yet supported.");

        (new SchemaConverter())->toAvroJsonSchema(new Schema(
            Schema\Definition::object('object', type_object(\stdClass::class, false))
        ));
    }
}
