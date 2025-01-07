<?php

use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\Logical\DateType;
use Flow\ETL\PHP\Type\Logical\JsonType;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Logical\TimeType;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\PHP\Type\Logical\XMLElementType;
use Flow\ETL\PHP\Type\Logical\XMLType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\BooleanType;
use Flow\ETL\PHP\Type\Native\CallableType;
use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Native\FloatType;
use Flow\ETL\PHP\Type\Native\IntegerType;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ResourceType;
use Flow\ETL\PHP\Type\Native\StringType;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\DatetimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\TimeEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Rows;
use Flow\Tools\Rector\NewObjectToFunction;
use Flow\Tools\Rector\NewToFunctionCallRector;
use Rector\Config\RectorConfig;
use Rector\DeadCode\Rector\StaticCall\RemoveParentCallWithoutParentRector;
use Rector\Set\ValueObject\LevelSetList;
use Rector\Transform\Rector\StaticCall\StaticCallToFuncCallRector;
use \Rector\Transform\ValueObject\StaticCallToFuncCall;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/src/core/etl/tests',
        __DIR__ . '/src/cli/tests',
        __DIR__ . '/src/adapter/*/tests',
        __DIR__ . '/src/bridge/*/*/tests',
        __DIR__ . '/src/tools/*/*/tests',
    ])
    ->withSets([
        LevelSetList::UP_TO_PHP_82
    ])
    ->withConfiguredRule(
        StaticCallToFuncCallRector::class,
        [
            // Building Blocks
            new StaticCallToFuncCall(Flow\ETL\Row::class, 'create', 'Flow\ETL\DSL\row'),
            new StaticCallToFuncCall(Config::class, 'default', 'Flow\ETL\DSL\config'),
            // Schema
            new StaticCallToFuncCall(Definition::class, 'boolean', 'Flow\ETL\DSL\bool_schema'),
            new StaticCallToFuncCall(Definition::class, 'date', 'Flow\ETL\DSL\date_schema'),
            new StaticCallToFuncCall(Definition::class, 'datetime', 'Flow\ETL\DSL\datetime_schema'),
            new StaticCallToFuncCall(Definition::class, 'enum', 'Flow\ETL\DSL\enum_schema'),
            new StaticCallToFuncCall(Definition::class, 'float', 'Flow\ETL\DSL\float_schema'),
            new StaticCallToFuncCall(Definition::class, 'integer', 'Flow\ETL\DSL\integer_schema'),
            new StaticCallToFuncCall(Definition::class, 'json', 'Flow\ETL\DSL\json_schema'),
            new StaticCallToFuncCall(Definition::class, 'list', 'Flow\ETL\DSL\list_schema'),
            new StaticCallToFuncCall(Definition::class, 'map', 'Flow\ETL\DSL\map_schema'),
            new StaticCallToFuncCall(Definition::class, 'string', 'Flow\ETL\DSL\string_schema'),
            new StaticCallToFuncCall(Definition::class, 'structure', 'Flow\ETL\DSL\structure_schema'),
            new StaticCallToFuncCall(Definition::class, 'time', 'Flow\ETL\DSL\time_schema'),
            new StaticCallToFuncCall(Definition::class, 'uuid', 'Flow\ETL\DSL\uuid_schema'),
            new StaticCallToFuncCall(Definition::class, 'xml', 'Flow\ETL\DSL\xml_schema'),
            new StaticCallToFuncCall(Definition::class, 'xml_element', 'Flow\ETL\DSL\xml_element_schema'),
            // Logical Types
            new StaticCallToFuncCall(MapKey::class, 'integer', 'Flow\ETL\DSL\type_integer'),
            new StaticCallToFuncCall(MapKey::class, 'string', 'Flow\ETL\DSL\type_string'),
            new StaticCallToFuncCall(MapValue::class, 'boolean', 'Flow\ETL\DSL\type_boolean'),
            new StaticCallToFuncCall(MapValue::class, 'datetime', 'Flow\ETL\DSL\type_datetime'),
            new StaticCallToFuncCall(MapValue::class, 'float', 'Flow\ETL\DSL\type_float'),
            new StaticCallToFuncCall(MapValue::class, 'integer', 'Flow\ETL\DSL\type_integer'),
            new StaticCallToFuncCall(MapValue::class, 'json', 'Flow\ETL\DSL\type_json'),
            new StaticCallToFuncCall(MapValue::class, 'list', 'Flow\ETL\DSL\type_list'),
            new StaticCallToFuncCall(MapValue::class, 'map', 'Flow\ETL\DSL\type_map'),
            new StaticCallToFuncCall(MapValue::class, 'object', 'Flow\ETL\DSL\type_object'),
            new StaticCallToFuncCall(MapValue::class, 'string', 'Flow\ETL\DSL\type_string'),
            new StaticCallToFuncCall(MapValue::class, 'structure', 'Flow\ETL\DSL\type_structure'),
            new StaticCallToFuncCall(MapValue::class, 'uuid', 'Flow\ETL\DSL\type_uuid'),
            new StaticCallToFuncCall(MapValue::class, 'xml', 'Flow\ETL\DSL\type_xml'),
            new StaticCallToFuncCall(MapValue::class, 'xmlElement', 'Flow\ETL\DSL\type_xml_element'),

            new StaticCallToFuncCall(ListElement::class, 'boolean', 'Flow\ETL\DSL\type_boolean'),
            new StaticCallToFuncCall(ListElement::class, 'datetime', 'Flow\ETL\DSL\type_datetime'),
            new StaticCallToFuncCall(ListElement::class, 'float', 'Flow\ETL\DSL\type_float'),
            new StaticCallToFuncCall(ListElement::class, 'integer', 'Flow\ETL\DSL\type_integer'),
            new StaticCallToFuncCall(ListElement::class, 'json', 'Flow\ETL\DSL\type_json'),
            new StaticCallToFuncCall(ListElement::class, 'list', 'Flow\ETL\DSL\type_list'),
            new StaticCallToFuncCall(ListElement::class, 'map', 'Flow\ETL\DSL\type_map'),
            new StaticCallToFuncCall(ListElement::class, 'object', 'Flow\ETL\DSL\type_object'),
            new StaticCallToFuncCall(ListElement::class, 'string', 'Flow\ETL\DSL\type_string'),
            new StaticCallToFuncCall(ListElement::class, 'structure', 'Flow\ETL\DSL\type_structure'),
            new StaticCallToFuncCall(ListElement::class, 'uuid', 'Flow\ETL\DSL\type_uuid'),
            new StaticCallToFuncCall(ListElement::class, 'xml', 'Flow\ETL\DSL\type_xml'),
            new StaticCallToFuncCall(ListElement::class, 'xmlElement', 'Flow\ETL\DSL\type_xml_element'),
        ]
    )
    ->withConfiguredRule(
        NewToFunctionCallRector::class,
        [
            // Building Blocks
            new NewObjectToFunction(Rows::class, 'Flow\ETL\DSL\rows'),
            new NewObjectToFunction(Config::class, 'Flow\ETL\DSL\config'),
            new NewObjectToFunction(FlowContext::class, 'Flow\ETL\DSL\flow_context'),
            new NewObjectToFunction(Flow\ETL\Row\Schema::class, 'Flow\ETL\DSL\schema'),

            // Entries
            new NewObjectToFunction(BooleanEntry::class, 'Flow\ETL\DSL\boolean_entry'),
            new NewObjectToFunction(DateEntry::class, 'Flow\ETL\DSL\date_entry'),
            new NewObjectToFunction(DatetimeEntry::class, 'Flow\ETL\DSL\datetime_entry'),
            new NewObjectToFunction(EnumEntry::class, 'Flow\ETL\DSL\enum_entry'),
            new NewObjectToFunction(FloatEntry::class, 'Flow\ETL\DSL\float_entry'),
            new NewObjectToFunction(IntegerEntry::class, 'Flow\ETL\DSL\integer_entry'),
            new NewObjectToFunction(JsonEntry::class, 'Flow\ETL\DSL\json_entry'),
            new NewObjectToFunction(ListEntry::class, 'Flow\ETL\DSL\list_entry'),
            new NewObjectToFunction(MapEntry::class, 'Flow\ETL\DSL\map_entry'),
            new NewObjectToFunction(StringEntry::class, 'Flow\ETL\DSL\string_entry'),
            new NewObjectToFunction(StructureEntry::class, 'Flow\ETL\DSL\structure_entry'),
            new NewObjectToFunction(TimeEntry::class, 'Flow\ETL\DSL\time_entry'),
            new NewObjectToFunction(UuidEntry::class, 'Flow\ETL\DSL\uuid_entry'),
            new NewObjectToFunction(XMLElementEntry::class, 'Flow\ETL\DSL\xml_element_entry'),
            new NewObjectToFunction(XMLEntry::class, 'Flow\ETL\DSL\xml_entry'),

            // Native Types
            new NewObjectToFunction(ArrayType::class, 'Flow\ETL\DSL\type_array'),
            new NewObjectToFunction(BooleanType::class, 'Flow\ETL\DSL\type_boolean'),
            new NewObjectToFunction(CallableType::class, 'Flow\ETL\DSL\type_callable'),
            new NewObjectToFunction(EnumType::class, 'Flow\ETL\DSL\type_enum'),
            new NewObjectToFunction(FloatType::class, 'Flow\ETL\DSL\type_float'),
            new NewObjectToFunction(IntegerType::class, 'Flow\ETL\DSL\type_integer'),
            new NewObjectToFunction(NullType::class, 'Flow\ETL\DSL\type_null'),
            new NewObjectToFunction(ObjectType::class, 'Flow\ETL\DSL\type_object'),
            new NewObjectToFunction(ResourceType::class, 'Flow\ETL\DSL\type_resource'),
            new NewObjectToFunction(StringType::class, 'Flow\ETL\DSL\type_string'),

            // Logical Types
            new NewObjectToFunction(DateTimeType::class, 'Flow\ETL\DSL\type_datetime'),
            new NewObjectToFunction(DateType::class, 'Flow\ETL\DSL\type_date'),
            new NewObjectToFunction(JsonType::class, 'Flow\ETL\DSL\type_json'),
            new NewObjectToFunction(ListType::class, 'Flow\ETL\DSL\type_list'),
            new NewObjectToFunction(MapType::class, 'Flow\ETL\DSL\type_map'),
            new NewObjectToFunction(StructureType::class, 'Flow\ETL\DSL\type_structure'),
            new NewObjectToFunction(TimeType::class, 'Flow\ETL\DSL\type_time'),
            new NewObjectToFunction(UuidType::class, 'Flow\ETL\DSL\type_uuid'),
            new NewObjectToFunction(XMLElementType::class, 'Flow\ETL\DSL\type_xml_element'),
            new NewObjectToFunction(XMLType::class, 'Flow\ETL\DSL\type_xml'),
            new NewObjectToFunction(StructureElement::class, 'Flow\ETL\DSL\structure_element'),

            // Extractors
            new NewObjectToFunction(Flow\ETL\Extractor\CacheExtractor::class, 'from_cache'),
            new NewObjectToFunction(Flow\ETL\Extractor\RowsExtractor::class, 'from_rows'),
            new NewObjectToFunction(Flow\ETL\Extractor\ArrayExtractor::class, 'from_array'),
            new NewObjectToFunction(Flow\ETL\Extractor\ChainExtractor::class, 'from_all'),
            new NewObjectToFunction(Flow\ETL\Extractor\MemoryExtractor::class, 'from_memory'),
            new NewObjectToFunction(Flow\ETL\Extractor\ChunkExtractor::class, 'chunks_from'),
            new NewObjectToFunction(Flow\ETL\Extractor\PipelineExtractor::class, 'from_pipeline'),
            new NewObjectToFunction(Flow\ETL\Extractor\DataFrameExtractor::class, 'from_data_frame'),

        ]
    )
    ->withSkip([
        RemoveParentCallWithoutParentRector::class
    ])
    ->withCache(__DIR__ . '/var/rector/tests')
    ->withImportNames(
        importNames: true,
        importDocBlockNames: true,
        importShortClasses: false,
        removeUnusedImports: true
    );