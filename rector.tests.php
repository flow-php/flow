<?php

use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchExtractor;
use Flow\ETL\Adapter\Excel\ExcelExtractor;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Adapter\Http\PsrHttpClientStaticExtractor;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonLinesExtractor;
use Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchExtractor;
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Adapter\XML\XMLParserExtractor;
use Flow\ETL\Config;
use Flow\ETL\Extractor\ArrayExtractor;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Extractor\ChainExtractor;
use Flow\ETL\Extractor\BatchExtractor;
use Flow\ETL\Extractor\DataFrameExtractor;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Extractor\PipelineExtractor;
use Flow\ETL\Extractor\RowsExtractor;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\CallableType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Native\ResourceType;
use Flow\Types\Type\Native\StringType;
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
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Rows;
use Flow\Tools\Rector\NewObjectToFunction;
use Flow\Tools\Rector\NewToFunctionCallRector;
use Rector\Config\RectorConfig;
use Rector\DeadCode\Rector\StaticCall\RemoveParentCallWithoutParentRector;
use Rector\Set\ValueObject\LevelSetList;
use Rector\Transform\Rector\StaticCall\StaticCallToFuncCallRector;
use \Rector\Transform\ValueObject\StaticCallToFuncCall;
use Rector\PHPUnit\AnnotationsToAttributes\Rector\ClassMethod\DataProviderAnnotationToAttributeRector;
use Rector\Php71\Rector\FuncCall\RemoveExtraParametersRector;
use Rector\CodingStyle\Rector\FuncCall\FunctionFirstClassCallableRector;
use Rector\CodingStyle\Rector\FunctionLike\FunctionLikeToFirstClassCallableRector;
use Flow\Filesystem\Path;
use Flow\ETL\Window;
use Flow\ETL\Function\RowNumber;
use Flow\ETL\Function\Min;
use Flow\ETL\Function\Max;
use Flow\ETL\Function\Sum;
use Flow\ETL\Function\Average;

return RectorConfig::configure()
    ->withPaths([
        __DIR__ . '/src/core/etl/tests',
        __DIR__ . '/src/cli/tests',
        __DIR__ . '/src/lib/*/*/tests',
        __DIR__ . '/src/adapter/*/*/tests',
        __DIR__ . '/src/bridge/*/*/tests',
        __DIR__ . '/src/tools/*/*/tests',
        __DIR__ . '/web/landing/tests',
    ])
    ->withSets([
        LevelSetList::UP_TO_PHP_82,
    ])
    ->withRules([
        DataProviderAnnotationToAttributeRector::class,
    ])
    ->withConfiguredRule(
        StaticCallToFuncCallRector::class,
        [
            // Building Blocks
            new StaticCallToFuncCall(Row::class, 'create', 'Flow\ETL\DSL\row'),
            new StaticCallToFuncCall(Path::class, 'from', 'Flow\Filesystem\DSL\path'),
            new StaticCallToFuncCall(Path::class, 'realpath', 'Flow\Filesystem\DSL\path_real'),
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
        ]
    )
    ->withConfiguredRule(
        NewToFunctionCallRector::class,
        [
            // Building Blocks
            new NewObjectToFunction(Rows::class, 'Flow\ETL\DSL\rows'),
            new NewObjectToFunction(Config::class, 'Flow\ETL\DSL\config'),
            new NewObjectToFunction(FlowContext::class, 'Flow\ETL\DSL\flow_context'),
            new NewObjectToFunction(Schema::class, 'Flow\ETL\DSL\schema'),
            new NewObjectToFunction(Flow::class, 'Flow\ETL\DSL\data_frame'),
            new NewObjectToFunction(Window::class, 'Flow\ETL\DSL\window'),

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
            new NewObjectToFunction(InstanceOfType::class, 'Flow\ETL\DSL\type_object'),
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
            new NewObjectToFunction(HTMLType::class, 'Flow\ETL\DSL\type_html'),
            new NewObjectToFunction(HTMLElementType::class, 'Flow\ETL\DSL\type_html_element'),

            // Extractors
            new NewObjectToFunction(CacheExtractor::class, 'from_cache'),
            new NewObjectToFunction(RowsExtractor::class, 'from_rows'),
            new NewObjectToFunction(ArrayExtractor::class, 'from_array'),
            new NewObjectToFunction(ChainExtractor::class, 'from_all'),
            new NewObjectToFunction(MemoryExtractor::class, 'from_memory'),
            new NewObjectToFunction(BatchExtractor::class, 'batches'),
            new NewObjectToFunction(PipelineExtractor::class, 'from_pipeline'),
            new NewObjectToFunction(DataFrameExtractor::class, 'from_data_frame'),

            // Adapters
            new NewObjectToFunction(AvroExtractor::class, 'Flow\ETL\DSL\Adapter\Avro\from_avro'),
            new NewObjectToFunction(CSVExtractor::class, 'Flow\ETL\Adapter\CSV\from_csv'),
            new NewObjectToFunction(DbalQueryExtractor::class, 'Flow\ETL\Adapter\Doctrine\from_dbal_query'),
            new NewObjectToFunction(ElasticsearchExtractor::class, 'Flow\ETL\Adapter\Elasticsearch\from_es'),
            new NewObjectToFunction(ExcelExtractor::class, 'Flow\ETL\Adapter\Excel\from_excel'),
            new NewObjectToFunction(GoogleSheetExtractor::class, 'Flow\ETL\Adapter\GoogleSheet\from_google_sheet'),
            new NewObjectToFunction(PsrHttpClientDynamicExtractor::class, 'Flow\ETL\Adapter\Http\from_dynamic_http_requests'),
            new NewObjectToFunction(PsrHttpClientStaticExtractor::class, 'Flow\ETL\Adapter\Http\from_static_http_requests'),
            new NewObjectToFunction(JsonExtractor::class, 'Flow\ETL\Adapter\JSON\from_json'),
            new NewObjectToFunction(JsonLinesExtractor::class, 'Flow\ETL\Adapter\JSON\from_json_lines'),
            new NewObjectToFunction(MeilisearchExtractor::class, 'Flow\ETL\Adapter\Meilisearch\from_meilisearch'),
            new NewObjectToFunction(ParquetExtractor::class, 'Flow\ETL\Adapter\Parquet\from_parquet'),
            new NewObjectToFunction(TextExtractor::class, 'Flow\ETL\Adapter\Text\from_text'),
            new NewObjectToFunction(XMLParserExtractor::class, 'Flow\ETL\Adapter\XML\from_xml'),

            // Functions
            new NewObjectToFunction(RowNumber::class, 'Flow\ETL\DSL\row_number'),
            new NewObjectToFunction(Min::class, 'Flow\ETL\DSL\min'),
            new NewObjectToFunction(Max::class, 'Flow\ETL\DSL\min'),
            new NewObjectToFunction(Sum::class, 'Flow\ETL\DSL\sum'),
            new NewObjectToFunction(Average::class, 'Flow\ETL\DSL\average'),
        ]
    )
    ->withSkip([
        RemoveParentCallWithoutParentRector::class,
        RemoveExtraParametersRector::class,
        FunctionFirstClassCallableRector::class,
        FunctionLikeToFirstClassCallableRector::class,
    ])
    ->withCache(__DIR__ . '/var/rector/tests')
    ->withSkipPath(__DIR__ . '/src/lib/parquet/src/Flow/Parquet/Thrift')
    ->withImportNames(
        importNames: true,
        importDocBlockNames: true,
        importShortClasses: false,
        removeUnusedImports: true
    );
