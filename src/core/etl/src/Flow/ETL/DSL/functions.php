<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\ErrorHandler\{IgnoreError, SkipRows, ThrowError};
use Flow\ETL\Exception\{InvalidArgumentException,
    RuntimeException,
    SchemaDefinitionNotFoundException};
use Flow\ETL\Extractor\FilesExtractor;
use Flow\ETL\Filesystem\{SaveMode};
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\Function\{All,
    Any,
    ArrayGet,
    ArrayGetCollection,
    ArrayKeyRename,
    ArrayKeysStyleConvert,
    ArrayMerge,
    ArrayMergeCollection,
    ArrayPathExists,
    ArrayReverse,
    ArraySort,
    ArrayUnpack,
    Average,
    Between,
    Capitalize,
    Cast,
    Coalesce,
    Collect,
    CollectUnique,
    Combine,
    Concat,
    Count,
    DateTimeFormat,
    DenseRank,
    Exists,
    First,
    Hash,
    Last,
    ListFunctions,
    Literal,
    Max,
    Min,
    Not,
    Now,
    NumberFormat,
    Optional,
    RandomString,
    Rank,
    Regex,
    RegexAll,
    RegexMatch,
    RegexMatchAll,
    RegexReplace,
    Round,
    RowNumber,
    Sanitize,
    ScalarFunction,
    Size,
    Split,
    Sprintf,
    StructureFunctions,
    Sum,
    ToDate,
    ToDateTime,
    ToLower,
    ToTimeZone,
    ToUpper,
    Ulid,
    Uuid,
    When};
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Loader\{ArrayLoader, CallbackLoader, MemoryLoader, StreamLoader, TransformerLoader};
use Flow\ETL\Memory\Memory;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{
    DateTimeType,
    DateType,
    JsonType,
    ListType,
    MapType,
    StructureType,
    TimeType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\ETL\PHP\Type\Native\{ArrayType, CallableType, EnumType, NullType, ObjectType, ResourceType, ScalarType};
use Flow\ETL\PHP\Type\{Type, TypeDetector};
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Schema\{Definition, Matcher\EvolvingSchemaMatcher, Matcher\StrictSchemaMatcher, SchemaFormatter};
use Flow\ETL\Row\{Entry, EntryFactory, EntryReference, Reference, References, Schema};
use Flow\ETL\{Attribute\DocumentationDSL,
    Attribute\DocumentationExample,
    Attribute\Module,
    Attribute\Type as DSLType,
    Cache\Implementation\FilesystemCache,
    Config,
    Config\ConfigBuilder,
    DataFrame,
    Extractor,
    Extractor\ArrayExtractor,
    Flow,
    FlowContext,
    Formatter,
    Hash\Algorithm,
    Hash\NativePHPHash,
    Join\Comparison,
    Join\Comparison\Equal,
    Join\Comparison\Identical,
    Join\Expression,
    Loader,
    NativePHPRandomValueGenerator,
    Pipeline,
    RandomValueGenerator,
    Row,
    Rows,
    Transformer,
    Window};
use Flow\Filesystem\Stream\Mode;
use Flow\Filesystem\{Filesystem, Local\NativeLocalFilesystem, Partition, Partitions, Path};
use Flow\Serializer\{NativePHPSerializer, Serializer};

/**
 * Alias for data_frame() : Flow.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'data_frame', example: 'data_frame')]
#[DocumentationExample(topic: 'data_frame', example: 'overwrite')]
function df(Config|ConfigBuilder|null $config = null) : Flow
{
    return data_frame($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'data_frame', example: 'data_frame')]
#[DocumentationExample(topic: 'data_frame', example: 'overwrite')]
function data_frame(Config|ConfigBuilder|null $config = null) : Flow
{
    return new Flow($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_frame', example: 'data_frame')]
#[DocumentationExample(topic: 'data_frame', example: 'overwrite')]
function from_rows(Rows ...$rows) : Extractor\RowsExtractor
{
    return new Extractor\RowsExtractor(...$rows);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'partitioning', example: 'path_partitions')]
function from_path_partitions(Path|string $path) : Extractor\PathPartitionsExtractor
{
    return new Extractor\PathPartitionsExtractor(\is_string($path) ? \Flow\Filesystem\DSL\path($path) : $path);
}

/**
 * @param iterable $array
 * @param null|Schema $schema - @deprecated use withSchema() method instead
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_reading', example: 'array')]
#[DocumentationExample(topic: 'data_frame', example: 'data_frame')]
function from_array(iterable $array, ?Schema $schema = null) : ArrayExtractor
{
    $extractor = new ArrayExtractor($array);

    if ($schema !== null) {
        $extractor->withSchema($schema);
    }

    return $extractor;
}

/**
 * @param string $id - cache id from which data will be extracted
 * @param null|Extractor $fallback_extractor - extractor that will be used when cache is empty - @deprecated use withFallbackExtractor() method instead
 * @param bool $clear - clear cache after extraction - @deprecated use withClearOnFinish() method instead
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_cache(string $id, ?Extractor $fallback_extractor = null, bool $clear = false) : Extractor\CacheExtractor
{
    $extractor = new Extractor\CacheExtractor($id);

    if ($fallback_extractor !== null) {
        $extractor->withFallbackExtractor($fallback_extractor);
    }

    if ($clear) {
        $extractor->withClearOnFinish($clear);
    }

    return $extractor;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_all(Extractor ...$extractors) : Extractor\ChainExtractor
{
    return new Extractor\ChainExtractor(...$extractors);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_memory(Memory $memory) : Extractor\MemoryExtractor
{
    return new Extractor\MemoryExtractor($memory);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function files(string|Path $directory) : FilesExtractor
{
    return new FilesExtractor(\is_string($directory) ? \Flow\Filesystem\DSL\path($directory) : $directory);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function filesystem_cache(Path|string|null $cache_dir = null, Filesystem $filesystem = new NativeLocalFilesystem(), Serializer $serializer = new NativePHPSerializer()) : FilesystemCache
{
    return new FilesystemCache($filesystem, $serializer, \is_string($cache_dir) ? Path::realpath($cache_dir) : $cache_dir);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function chunks_from(Extractor $extractor, int $chunk_size) : Extractor\ChunkExtractor
{
    return new Extractor\ChunkExtractor($extractor, $chunk_size);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_pipeline(Pipeline $pipeline) : Extractor\PipelineExtractor
{
    return new Extractor\PipelineExtractor($pipeline);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_data_frame(DataFrame $data_frame) : Extractor\DataFrameExtractor
{
    return new Extractor\DataFrameExtractor($data_frame);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, \DateTimeInterface $end, int $options = 0) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $end, $options)),
        $entry_name
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period_recurrences(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, int $recurrences, int $options = 0) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $recurrences, $options)),
        $entry_name
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_number(string $entry_name, string|int|float $start, string|int|float $end, int|float $step = 1) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\NumberSequenceGenerator($start, $end, $step),
        $entry_name
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_callable(callable $callable) : CallbackLoader
{
    return new CallbackLoader($callable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_memory(Memory $memory) : MemoryLoader
{
    return new MemoryLoader($memory);
}

/**
 * Convert rows to an array and store them in passed array variable.
 *
 * @param-out array<array<mixed>> $array
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
#[DocumentationExample(topic: 'data_writing', example: 'array')]
function to_array(array &$array) : ArrayLoader
{
    return new ArrayLoader($array);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_output(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::output($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stderr(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stderr($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stdout(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stdout($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stream(string $uri, int|bool $truncate = 20, Output $output = Output::rows, string $mode = 'w', Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return new StreamLoader($uri, Mode::from($mode), $truncate, $output, $formatter, $schemaFormatter, StreamLoader\Type::custom);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_transformation(Transformer $transformer, Loader $loader) : TransformerLoader
{
    return new TransformerLoader($transformer, $loader);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_branch(ScalarFunction $condition, Loader $loader) : Loader
{
    return new Loader\BranchingLoader($condition, $loader);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function bool_entry(string $name, ?bool $value) : Entry\BooleanEntry
{
    return new Entry\BooleanEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function boolean_entry(string $name, ?bool $value) : Entry\BooleanEntry
{
    return bool_entry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function datetime_entry(string $name, \DateTimeInterface|string|null $value) : Entry\DateTimeEntry
{
    return new Entry\DateTimeEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function time_entry(string $name, \DateInterval|string|null $value) : Entry\TimeEntry
{
    return new Entry\TimeEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function date_entry(string $name, \DateTimeInterface|string|null $value) : Entry\DateEntry
{
    return new Entry\DateEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function int_entry(string $name, ?int $value) : Entry\IntegerEntry
{
    return new Entry\IntegerEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function integer_entry(string $name, ?int $value) : Entry\IntegerEntry
{
    return int_entry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function enum_entry(string $name, ?\UnitEnum $enum) : Entry\EnumEntry
{
    return new Entry\EnumEntry($name, $enum);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function float_entry(string $name, ?float $value) : Entry\FloatEntry
{
    return new Entry\FloatEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_entry(string $name, array|string|null $data) : Entry\JsonEntry
{
    return new Entry\JsonEntry($name, $data);
}

/**
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_object_entry(string $name, array|string|null $data) : Entry\JsonEntry
{
    if (\is_string($data)) {
        return new Entry\JsonEntry($name, $data);
    }

    return Entry\JsonEntry::object($name, $data);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function str_entry(string $name, ?string $value) : Entry\StringEntry
{
    return new Entry\StringEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function string_entry(string $name, ?string $value) : Entry\StringEntry
{
    return str_entry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function uuid_entry(string $name, \Flow\ETL\PHP\Value\Uuid|string|null $value) : Entry\UuidEntry
{
    return new Entry\UuidEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_entry(string $name, \DOMDocument|string|null $value) : Entry\XMLEntry
{
    return new Entry\XMLEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_element_entry(string $name, \DOMElement|string|null $value) : Entry\XMLElementEntry
{
    return new Entry\XMLElementEntry($name, $value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function entries(Entry ...$entries) : Row\Entries
{
    return new Row\Entries(...$entries);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function struct_entry(string $name, ?array $value, StructureType $type) : Entry\StructureEntry
{
    return new Entry\StructureEntry($name, $value, $type);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function structure_entry(string $name, ?array $value, StructureType $type) : Entry\StructureEntry
{
    return new Entry\StructureEntry($name, $value, $type);
}

/**
 * @param array<StructureElement> $elements
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function struct_type(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

/**
 * @param array<StructureElement> $elements
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function structure_type(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

/**
 * @param array<StructureElement> $elements
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_structure(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function struct_element(string $name, Type $type) : StructureElement
{
    return new StructureElement($name, $type);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function structure_element(string $name, Type $type) : StructureElement
{
    return new StructureElement($name, $type);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function list_entry(string $name, ?array $value, ListType $type) : Entry\ListEntry
{
    return new Entry\ListEntry($name, $value, $type);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_list(Type $element, bool $nullable = false) : ListType
{
    return new ListType(new ListElement($element), $nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_map(ScalarType $key_type, Type $value_type, bool $nullable = false) : MapType
{
    return new MapType(new MapKey($key_type), new MapValue($value_type), $nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function map_entry(string $name, ?array $value, MapType $mapType) : Entry\MapEntry
{
    return new Entry\MapEntry($name, $value, $mapType);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_json(bool $nullable = false) : JsonType
{
    return new JsonType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_datetime(bool $nullable = false) : DateTimeType
{
    return new DateTimeType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_date(bool $nullable = false) : DateType
{
    return new DateType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_time(bool $nullable = false) : TimeType
{
    return new TimeType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_xml(bool $nullable = false) : XMLType
{
    return new XMLType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_xml_element(bool $nullable = false) : XMLElementType
{
    return new XMLElementType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_uuid(bool $nullable = false) : UuidType
{
    return new UuidType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_int(bool $nullable = false) : ScalarType
{
    return ScalarType::integer($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_integer(bool $nullable = false) : ScalarType
{
    return ScalarType::integer($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_string(bool $nullable = false) : ScalarType
{
    return ScalarType::string($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_float(bool $nullable = false) : ScalarType
{
    return ScalarType::float($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_boolean(bool $nullable = false) : ScalarType
{
    return ScalarType::boolean($nullable);
}

/**
 * @param class-string $class
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_object(string $class, bool $nullable = false) : ObjectType
{
    return new ObjectType($class, $nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_resource(bool $nullable = true) : ResourceType
{
    return new ResourceType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_array(bool $empty = false, bool $nullable = false) : ArrayType
{
    return new ArrayType($empty, $nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_callable(bool $nullable = true) : CallableType
{
    return new CallableType($nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_null() : NullType
{
    return new NullType();
}

/**
 * @param class-string<\UnitEnum> $class
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_enum(string $class, bool $nullable = false) : EnumType
{
    return new EnumType($class, $nullable);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function row(Entry ...$entry) : Row
{
    return Row::create(...$entry);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function rows(Row ...$row) : Rows
{
    return new Rows(...$row);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function rows_partitioned(array $rows, array|Partitions $partitions) : Rows
{
    return Rows::partitioned($rows, $partitions);
}

/**
 * An alias for `ref`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function col(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

/**
 * An alias for `ref`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
#[DocumentationExample(topic: 'data_frame', example: 'create_columns')]
function entry(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
#[DocumentationExample(topic: 'data_frame', example: 'create_columns')]
function ref(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure_ref(string $entry) : StructureFunctions
{
    return ref($entry)->structure();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function list_ref(string $entry) : ListFunctions
{
    return ref($entry)->list();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function refs(string|Reference ...$entries) : References
{
    return new References(...$entries);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function optional(ScalarFunction $function) : Optional
{
    return new Optional($function);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
#[DocumentationExample(topic: 'data_frame', example: 'create_columns')]
function lit(mixed $value) : Literal
{
    return new Literal($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function exists(ScalarFunction $ref) : Exists
{
    return new Exists($ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function when(mixed $condition, mixed $then, mixed $else = null) : When
{
    return new When($condition, $then, $else);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get(ScalarFunction $ref, ScalarFunction|string $path) : ArrayGet
{
    return new ArrayGet($ref, $path);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get_collection(ScalarFunction $ref, ScalarFunction|array $keys) : ArrayGetCollection
{
    return new ArrayGetCollection($ref, $keys);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get_collection_first(ScalarFunction $ref, string ...$keys) : ArrayGetCollection
{
    return ArrayGetCollection::fromFirst($ref, $keys);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_exists(ScalarFunction|array $ref, ScalarFunction|string $path) : ArrayPathExists
{
    return new ArrayPathExists($ref, $path);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_merge(ScalarFunction|array $left, ScalarFunction|array $right) : ArrayMerge
{
    return new ArrayMerge($left, $right);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_merge_collection(ScalarFunction|array $array) : ArrayMergeCollection
{
    return new ArrayMergeCollection($array);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_key_rename(ScalarFunction $ref, ScalarFunction|string $path, ScalarFunction|string $newName) : ArrayKeyRename
{
    return new ArrayKeyRename($ref, $path, $newName);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_keys_style_convert(ScalarFunction $ref, StringStyles|string $style = StringStyles::SNAKE) : ArrayKeysStyleConvert
{
    return new ArrayKeysStyleConvert($ref, $style instanceof StringStyles ? $style : StringStyles::fromString($style));
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_sort(ScalarFunction $function, ScalarFunction|Sort|null $sort_function = null, ScalarFunction|int|null $flags = null, ScalarFunction|bool $recursive = true) : ArraySort
{
    if ($sort_function === null) {
        $sort_function = Sort::sort;
    }

    return new ArraySort($function, $sort_function, $flags, $recursive);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_reverse(ScalarFunction|array $function, ScalarFunction|bool $preserveKeys = false) : ArrayReverse
{
    return new ArrayReverse($function, $preserveKeys);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function now(\DateTimeZone|ScalarFunction $time_zone = new \DateTimeZone('UTC')) : Now
{
    return new Now($time_zone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function between(mixed $value, mixed $lower_bound, mixed $upper_bound, ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE) : Between
{
    return new Between($value, $lower_bound, $upper_bound, $boundary);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_date_time(mixed $ref, ScalarFunction|string $format = 'Y-m-d H:i:s', ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDateTime
{
    return new ToDateTime($ref, $format, $timeZone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_date(mixed $ref, ScalarFunction|string $format = 'Y-m-d', ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDate
{
    return new ToDate($ref, $format, $timeZone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function date_time_format(ScalarFunction $ref, string $format) : DateTimeFormat
{
    return new DateTimeFormat($ref, $format);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function split(ScalarFunction|string $value, ScalarFunction|string $separator, ScalarFunction|int $limit = PHP_INT_MAX) : Split
{
    return new Split($value, $separator, $limit);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function combine(ScalarFunction|array $keys, ScalarFunction|array $values) : Combine
{
    return new Combine($keys, $values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function concat(ScalarFunction|string ...$functions) : Concat
{
    return new Concat(...$functions);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function hash(mixed $value, Algorithm $algorithm = new NativePHPHash()) : Hash
{
    return new Hash($value, $algorithm);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function cast(mixed $value, ScalarFunction|string|Type $type) : Cast
{
    return new Cast($value, $type);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function coalesce(ScalarFunction ...$values) : Coalesce
{
    return new Coalesce(...$values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function count(EntryReference $function) : Count
{
    return new Count($function);
}

/**
 * Unpacks each element of an array into a new entry, using the array key as the entry name.
 *
 * Before:
 * +--+-------------------+
 * |id|              array|
 * +--+-------------------+
 * | 1|{"a":1,"b":2,"c":3}|
 * | 2|{"d":4,"e":5,"f":6}|
 * +--+-------------------+
 *
 * After:
 * +--+-----+-----+-----+-----+-----+
 * |id|arr.b|arr.c|arr.d|arr.e|arr.f|
 * +--+-----+-----+-----+-----+-----+
 * | 1|    2|    3|     |     |     |
 * | 2|     |     |    4|    5|    6|
 * +--+-----+-----+-----+-----+-----+
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_unpack(ScalarFunction|array $array, ScalarFunction|array $skip_keys = [], ScalarFunction|string|null $entry_prefix = null) : ArrayUnpack
{
    return new ArrayUnpack($array, $skip_keys, $entry_prefix);
}

/**
 * Expands each value into entry, if there are more than one value, multiple rows will be created.
 * Array keys are ignored, only values are used to create new rows.
 *
 * Before:
 *   +--+-------------------+
 *   |id|              array|
 *   +--+-------------------+
 *   | 1|{"a":1,"b":2,"c":3}|
 *   +--+-------------------+
 *
 * After:
 *   +--+--------+
 *   |id|expanded|
 *   +--+--------+
 *   | 1|       1|
 *   | 1|       2|
 *   | 1|       3|
 *   +--+--------+
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_expand(ScalarFunction $function, ArrayExpand $expand = ArrayExpand::VALUES) : \Flow\ETL\Function\ArrayExpand
{
    return new \Flow\ETL\Function\ArrayExpand($function, $expand);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function size(mixed $value) : Size
{
    return new Size($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function uuid_v4() : Uuid
{
    return Uuid::uuid4();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function uuid_v7(ScalarFunction|\DateTimeInterface|null $value = null) : Uuid
{
    return Uuid::uuid7($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function ulid(ScalarFunction|string|null $value = null) : Ulid
{
    return new Ulid($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function lower(ScalarFunction|string $value) : ToLower
{
    return new ToLower($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function capitalize(ScalarFunction|string $value) : Capitalize
{
    return new Capitalize($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function upper(ScalarFunction|string $value) : ToUpper
{
    return new ToUpper($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function all(ScalarFunction ...$functions) : All
{
    return new All(...$functions);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function any(ScalarFunction ...$values) : Any
{
    return new Any(...$values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function not(ScalarFunction $value) : Not
{
    return new Not($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_timezone(ScalarFunction|\DateTimeInterface $value, ScalarFunction|\DateTimeZone|string $timeZone) : ToTimeZone
{
    return new ToTimeZone($value, $timeZone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function ignore_error_handler() : IgnoreError
{
    return new IgnoreError();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function skip_rows_handler() : SkipRows
{
    return new SkipRows();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function throw_error_handler() : ThrowError
{
    return new ThrowError();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_replace(ScalarFunction|string $pattern, ScalarFunction|string $replacement, ScalarFunction|string $subject, ScalarFunction|int|null $limit = null) : RegexReplace
{
    return new RegexReplace($pattern, $replacement, $subject, $limit);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_match_all(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexMatchAll
{
    return new RegexMatchAll($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_match(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexMatch
{
    return new RegexMatch($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : Regex
{
    return new Regex($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_all(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexAll
{
    return new RegexAll($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function sprintf(ScalarFunction|string $format, ScalarFunction|float|int|string|null ...$args) : Sprintf
{
    return new Sprintf($format, ...$args);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function sanitize(ScalarFunction|string $value, ScalarFunction|string $placeholder = '*', ScalarFunction|int|null $skipCharacters = null) : Sanitize
{
    return new Sanitize($value, $placeholder, $skipCharacters);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function round(ScalarFunction|int|float $value, ScalarFunction|int $precision = 2, ScalarFunction|int $mode = PHP_ROUND_HALF_UP) : Round
{
    return new Round($value, $precision, $mode);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function number_format(ScalarFunction|int|float $value, ScalarFunction|int $decimals = 2, ScalarFunction|string $decimal_separator = '.', ScalarFunction|string $thousands_separator = ',') : NumberFormat
{
    return new NumberFormat($value, $decimals, $decimal_separator, $thousands_separator);
}

/**
 * @param array<mixed> $data
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function to_entry(string $name, mixed $data, EntryFactory $entryFactory = new NativeEntryFactory()) : Entry
{
    return $entryFactory->create($name, $data);
}

/**
 * @param array<array<mixed>>|array<mixed|string> $data
 * @param array<Partition>|Partitions $partitions
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function array_to_row(array $data, EntryFactory $entryFactory = new NativeEntryFactory(), array|Partitions $partitions = [], ?Schema $schema = null) : Row
{
    $entries = [];

    foreach ($data as $key => $value) {
        $name = \is_int($key) ? 'e' . \str_pad((string) $key, 2, '0', STR_PAD_LEFT) : $key;

        try {
            $entries[$name] = $entryFactory->create($name, $value, $schema);
        } catch (SchemaDefinitionNotFoundException $e) {
            if ($schema === null) {
                throw $e;
            }
        }
    }

    foreach ($partitions as $partition) {
        if (!\array_key_exists($partition->name, $entries)) {
            try {
                $entries[$partition->name] = $entryFactory->create($partition->name, $partition->value, $schema);
            } catch (SchemaDefinitionNotFoundException $e) {
                if ($schema === null) {
                    throw $e;
                }
            }
        }
    }

    if ($schema !== null) {
        foreach ($schema->definitions() as $definition) {
            if (!\array_key_exists($definition->entry()->name(), $entries)) {
                $entries[$definition->entry()->name()] = str_entry($definition->entry()->name(), null);
            }
        }
    }

    return Row::create(...\array_values($entries));
}

/**
 * @param array<array<mixed>>|array<mixed|string> $data
 * @param array<Partition>|Partitions $partitions
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function array_to_rows(array $data, EntryFactory $entryFactory = new NativeEntryFactory(), array|Partitions $partitions = [], ?Schema $schema = null) : Rows
{
    $partitions = \is_array($partitions) ? new Partitions(...$partitions) : $partitions;

    $isRows = true;

    foreach ($data as $v) {
        if (!\is_array($v)) {
            $isRows = false;

            break;
        }
    }

    if (!$isRows) {
        return Rows::partitioned([array_to_row($data, $entryFactory, $partitions, $schema)], $partitions);
    }

    $rows = [];

    foreach ($data as $row) {
        $rows[] = array_to_row($row, $entryFactory, $partitions, $schema);
    }

    return Rows::partitioned($rows, $partitions);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function rank() : Rank
{
    return new Rank();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function dens_rank() : DenseRank
{
    return dense_rank();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function dense_rank() : DenseRank
{
    return new DenseRank();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function average(EntryReference|string $ref) : Average
{
    return new Average(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect(EntryReference|string $ref) : Collect
{
    return new Collect(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect_unique(EntryReference|string $ref) : CollectUnique
{
    return new CollectUnique(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function window() : Window
{
    return new Window();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function sum(EntryReference|string $ref) : Sum
{
    return new Sum(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function first(EntryReference|string $ref) : First
{
    return new First(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function last(EntryReference|string $ref) : Last
{
    return new Last(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function max(EntryReference|string $ref) : Max
{
    return new Max(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function min(EntryReference|string $ref) : Min
{
    return new Min(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function row_number() : RowNumber
{
    return new RowNumber();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema(Definition ...$definitions) : Schema
{
    return new Schema(...$definitions);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_to_json(Schema $schema, int $json_flags = JSON_THROW_ON_ERROR) : string
{
    /**
     * @phpstan-ignore-next-line
     */
    return \json_encode($schema->normalize(), $json_flags);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_from_json(string $schema) : Schema
{
    return Schema::fromArray(\json_decode($schema, true, 512, JSON_THROW_ON_ERROR));
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_strict_matcher() : StrictSchemaMatcher
{
    return new StrictSchemaMatcher();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_evolving_matcher() : EvolvingSchemaMatcher
{
    return new EvolvingSchemaMatcher();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function int_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::integer($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function str_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::string($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function bool_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::boolean($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function float_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::float($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function map_schema(string $name, MapType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::map($name, $type, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function list_schema(string $name, ListType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::list($name, $type, $metadata);
}

/**
 * @param class-string<\UnitEnum> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function enum_schema(string $name, string $type, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::enum($name, $type, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function null_schema(string $name, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::string($name, true, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function datetime_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::datetime($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function time_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::time($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function date_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::date($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function json_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::json($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::xml($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_element_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::xml_element($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function struct_schema(string $name, StructureType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function structure_schema(string $name, StructureType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function uuid_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::uuid($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function execution_context(?Config $config = null) : FlowContext
{
    return new FlowContext($config ?? Config::default());
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function flow_context(?Config $config = null) : FlowContext
{
    return execution_context($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config() : Config
{
    return Config::default();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config_builder() : ConfigBuilder
{
    return new ConfigBuilder();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function overwrite() : SaveMode
{
    return SaveMode::Overwrite;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function ignore() : SaveMode
{
    return SaveMode::Ignore;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function exception_if_exists() : SaveMode
{
    return SaveMode::ExceptionIfExists;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function append() : SaveMode
{
    return SaveMode::Append;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function get_type(mixed $value) : Type
{
    return (new TypeDetector())->detectType($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function print_schema(Schema $schema, ?SchemaFormatter $formatter = null) : string
{
    return ($formatter ?? new ASCIISchemaFormatter())->format($schema);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function print_rows(Rows $rows, int|bool $truncate = false, ?Formatter $formatter = null) : string
{
    return ($formatter ?? new Formatter\AsciiTableFormatter())->format($rows, $truncate);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function identical(Reference|string $left, Reference|string $right) : Identical
{
    return new Identical($left, $right);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function equal(Reference|string $left, Reference|string $right) : Equal
{
    return new Equal($left, $right);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function compare_all(Comparison ...$comparisons) : Comparison\All
{
    return new Comparison\All(...$comparisons);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function compare_any(Comparison ...$comparisons) : Comparison\Any
{
    return new Comparison\Any(...$comparisons);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'join', example: 'join')]
#[DocumentationExample(topic: 'join', example: 'join_each')]
function join_on(array|Comparison $comparisons, string $join_prefix = '') : Expression
{
    return Expression::on($comparisons, $join_prefix);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_name(Transformer\OrderEntries\Order $order = Transformer\OrderEntries\Order::ASC) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\NameComparator($order);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_name_desc() : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\NameComparator(Transformer\OrderEntries\Order::DESC);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type(array $priorities = Transformer\OrderEntries\TypePriorities::PRIORITIES, Transformer\OrderEntries\Order $order = Transformer\OrderEntries\Order::ASC) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\TypeComparator(new Transformer\OrderEntries\TypePriorities($priorities), $order);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type_desc(array $priorities = Transformer\OrderEntries\TypePriorities::PRIORITIES) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\TypeComparator(new Transformer\OrderEntries\TypePriorities($priorities), Transformer\OrderEntries\Order::DESC);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type_and_name(array $priorities = Transformer\OrderEntries\TypePriorities::PRIORITIES, Transformer\OrderEntries\Order $order = Transformer\OrderEntries\Order::ASC) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\CombinedComparator(
        new Transformer\OrderEntries\TypeComparator(new Transformer\OrderEntries\TypePriorities($priorities), $order),
        new Transformer\OrderEntries\NameComparator($order)
    );
}

/**
 * @param array<string|Type> $types
 * @param mixed $value
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function is_type(array $types, mixed $value) : bool
{
    foreach ($types as $type) {
        if (\is_string($type)) {
            if (match (\strtolower($type)) {
                'str', 'string' => \is_string($value),
                'int', 'integer' => \is_int($value),
                'float' => \is_float($value),
                'null' => null === $value,
                'object' => \is_object($value),
                'array' => \is_array($value),
                'list' => \is_array($value) && \array_is_list($value),
                default => match (\class_exists($type) || \enum_exists($type)) {
                    true => $value instanceof $type,
                    false => throw new RuntimeException('Unexpected type: ' . $type),
                },
            }) {
                return true;
            }
        } else {
            if ($type->isValid($value)) {
                return true;
            }
        }
    }

    return false;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function generate_random_string(int $length = 32, NativePHPRandomValueGenerator $generator = new NativePHPRandomValueGenerator()) : string
{
    return $generator->string($length);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function generate_random_int(int $start = PHP_INT_MIN, int $end = PHP_INT_MAX, NativePHPRandomValueGenerator $generator = new NativePHPRandomValueGenerator()) : int
{
    return $generator->int($start, $end);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function random_string(
    int|ScalarFunction $length,
    RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
) : RandomString {
    return new RandomString($length, $generator);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function dom_element_to_string(\DOMElement $element, bool $format_output = false, bool $preserver_white_space = false) : string|false
{
    $doc = new \DOMDocument('1.0', 'UTF-8');
    $doc->formatOutput = $format_output;
    $doc->preserveWhiteSpace = $preserver_white_space;

    $importedNode = $doc->importNode($element, true);
    $doc->appendChild($importedNode);

    return $doc->saveXML($doc->documentElement);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function date_interval_to_milliseconds(\DateInterval $interval) : int
{
    if ($interval->y !== 0 || $interval->m !== 0) {
        throw new InvalidArgumentException("Relative DateInterval (with months/years) can't be converted to milliseconds. Given" . \json_encode($interval, JSON_THROW_ON_ERROR));
    }

    $absoluteSeconds = $interval->d * 24 * 60 * 60
        + $interval->h * 60 * 60
        + $interval->i * 60
        + $interval->s;

    return $interval->invert
        ? -(int) ($absoluteSeconds * 1000 + $interval->f * 1000)
        : (int) ($absoluteSeconds * 1000 + $interval->f * 1000);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function date_interval_to_seconds(\DateInterval $interval) : int
{
    if ($interval->y !== 0 || $interval->m !== 0) {
        throw new InvalidArgumentException("Relative DateInterval (with months/years) can't be converted to seconds. Given" . \json_encode($interval, JSON_THROW_ON_ERROR));
    }

    $absoluteSeconds = $interval->d * 24 * 60 * 60
        + $interval->h * 60 * 60
        + $interval->i * 60
        + $interval->s;

    return $interval->invert
        ? -(int) ceil($absoluteSeconds + $interval->f)
        : (int) ceil($absoluteSeconds + $interval->f);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function date_interval_to_microseconds(\DateInterval $interval) : int
{
    if ($interval->y !== 0 || $interval->m !== 0) {
        throw new InvalidArgumentException("Relative DateInterval (with months/years) can't be converted to microseconds. Given" . \json_encode($interval, JSON_THROW_ON_ERROR));
    }

    $absoluteSeconds = $interval->d * 24 * 60 * 60
        + $interval->h * 60 * 60
        + $interval->i * 60
        + $interval->s;

    return $interval->invert
        ? -(int) ($absoluteSeconds * 1000000 + $interval->f * 1000000)
        : (int) ($absoluteSeconds * 1000000 + $interval->f * 1000000);
}
