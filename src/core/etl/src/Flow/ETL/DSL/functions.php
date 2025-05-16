<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use function Flow\Types\DSL\{is_nullable as is_nullable_new, type_array as type_array_new, type_boolean as type_boolean_new, type_callable as type_callable_new, type_date as type_date_new, type_datetime as type_datetime_new, type_enum as type_enum_new, type_equals as type_equals_new, type_float as type_float_new, type_from_array as type_from_array_new, type_instance_of as type_instance_of_new, type_integer as type_integer_new, type_is as type_is_new, type_is_any as type_is_any_new, type_json as type_json_new, type_list as type_list_new, type_map as type_map_new, type_null as type_null_new, type_optional as type_optional_new, type_resource as type_resource_new, type_string as type_string_new, type_structure as type_structure_new, type_time as type_time_new, type_union as type_union_new, type_uuid as type_uuid_new, type_xml as type_xml_new, type_xml_element as type_xml_element_new, types as types_new};
use Flow\Calculator\Rounding;
use Flow\ETL\{
    Analyze,
    Attribute\DocumentationDSL,
    Attribute\DocumentationExample,
    Attribute\Module,
    Attribute\Type as DSLType,
    Cache\Implementation\FilesystemCache,
    Config,
    Config\ConfigBuilder,
    Constraint\UniqueConstraint,
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
    Schema,
    SchemaValidator,
    Schema\SchemaFormatter,
    String\StringStyles,
    Transformation,
    Transformer,
    Window,
    WithEntry
};
use Flow\ETL\ErrorHandler\{IgnoreError, SkipRows, ThrowError};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException, SchemaDefinitionNotFoundException};
use Flow\ETL\Extractor\{CacheExtractor, ChainExtractor, ChunkExtractor, DataFrameExtractor, FilesExtractor, MemoryExtractor, PathPartitionsExtractor, PipelineExtractor, RowsExtractor, SequenceExtractor};
use Flow\ETL\Extractor\SequenceGenerator\{DatePeriodSequenceGenerator, NumberSequenceGenerator};
use Flow\ETL\Filesystem\{SaveMode};
use Flow\ETL\Formatter\AsciiTableFormatter;
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
    ConcatWithSeparator,
    Count,
    DateTimeFormat,
    DenseRank,
    Exists,
    First,
    Greatest,
    Hash,
    Last,
    Least,
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
    StringAggregate,
    StructureFunctions,
    StyleConverter\StringStyles as OldStringStyles,
    Sum,
    ToDate,
    ToDateTime,
    ToLower,
    ToTimeZone,
    ToUpper,
    Ulid,
    Uuid,
    When};
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Loader\{ArrayLoader, CallbackLoader, MemoryLoader, StreamLoader, TransformerLoader};
use Flow\ETL\Loader\BranchingLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Row\{Entries, EntryFactory, SortOrder};
use Flow\ETL\Row\Entry\{BooleanEntry, DateEntry, DateTimeEntry, EnumEntry, FloatEntry, IntegerEntry, JsonEntry, ListEntry, MapEntry, StringEntry, StructureEntry, TimeEntry, UuidEntry, XMLElementEntry, XMLEntry};
use Flow\ETL\Row\{Entry, EntryReference, Reference, References};
use Flow\ETL\Row\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Schema\{Definition};
use Flow\ETL\Schema\Formatter\JsonSchemaFormatter;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Schema\Validator\{EvolvingValidator, SelectiveValidator, StrictValidator};
use Flow\ETL\Transformer\OrderEntries\{CombinedComparator, Comparator, NameComparator, Order, TypeComparator, TypePriorities};
use Flow\ETL\Transformer\Rename\{RenameCaseEntryStrategy, RenameReplaceEntryStrategy};
use Flow\Filesystem\{Filesystem, Local\NativeLocalFilesystem, Partition, Partitions, Path};
use Flow\Filesystem\Stream\Mode;
use Flow\Serializer\{NativePHPSerializer, Serializer};
use Flow\Types\Type\Logical\{DateTimeType,
    DateType,
    InstanceOfType,
    JsonType,
    ListType,
    MapType,
    OptionalType,
    StructureType,
    TimeType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\Types\Type\Native\{
    ArrayType,
    BooleanType,
    CallableType,
    EnumType,
    FloatType,
    IntegerType,
    NullType,
    ResourceType,
    StringType,
    UnionType
};
use Flow\Types\Type\{Type, TypeDetector, Types};
use UnitEnum;

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
function from_rows(Rows ...$rows) : RowsExtractor
{
    return new RowsExtractor(...$rows);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'partitioning', example: 'path_partitions')]
function from_path_partitions(Path|string $path) : PathPartitionsExtractor
{
    return new PathPartitionsExtractor(\is_string($path) ? \Flow\Filesystem\DSL\path($path) : $path);
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
function from_cache(string $id, ?Extractor $fallback_extractor = null, bool $clear = false) : CacheExtractor
{
    $extractor = new CacheExtractor($id);

    if ($fallback_extractor !== null) {
        $extractor->withFallbackExtractor($fallback_extractor);
    }

    if ($clear) {
        $extractor->withClearOnFinish($clear);
    }

    return $extractor;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_all(Extractor ...$extractors) : ChainExtractor
{
    return new ChainExtractor(...$extractors);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_memory(Memory $memory) : MemoryExtractor
{
    return new MemoryExtractor($memory);
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

/**
 * @param int<1, max> $chunk_size
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function chunks_from(Extractor $extractor, int $chunk_size) : ChunkExtractor
{
    return new ChunkExtractor($extractor, $chunk_size);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_pipeline(Pipeline $pipeline) : PipelineExtractor
{
    return new PipelineExtractor($pipeline);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_data_frame(DataFrame $data_frame) : DataFrameExtractor
{
    return new DataFrameExtractor($data_frame);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, \DateTimeInterface $end, int $options = 0) : SequenceExtractor
{
    return new SequenceExtractor(
        new DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $end, $options)),
        $entry_name
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period_recurrences(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, int $recurrences, int $options = 0) : SequenceExtractor
{
    return new SequenceExtractor(
        new DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $recurrences, $options)),
        $entry_name
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_number(string $entry_name, string|int|float $start, string|int|float $end, int|float $step = 1) : SequenceExtractor
{
    return new SequenceExtractor(
        new NumberSequenceGenerator($start, $end, $step),
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
function to_output(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::output($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stderr(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stderr($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stdout(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stdout($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stream(string $uri, int|bool $truncate = 20, Output $output = Output::rows, string $mode = 'w', Formatter $formatter = new AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return new StreamLoader($uri, Mode::from($mode), $truncate, $output, $formatter, $schemaFormatter, StreamLoader\Type::custom);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_transformation(Transformer|Transformation $transformer, Loader $loader) : TransformerLoader
{
    return new TransformerLoader($transformer, $loader);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_branch(ScalarFunction $condition, Loader $loader) : BranchingLoader
{
    return new BranchingLoader($condition, $loader);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_style(OldStringStyles|StringStyles $style) : RenameCaseEntryStrategy
{
    return new RenameCaseEntryStrategy($style);
}

/**
 * @param array<string>|string $search
 * @param array<string>|string $replace
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_replace(string|array $search, string|array $replace) : RenameReplaceEntryStrategy
{
    return new RenameReplaceEntryStrategy($search, $replace);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function bool_entry(string $name, ?bool $value, ?Metadata $metadata = null) : BooleanEntry
{
    return new BooleanEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function boolean_entry(string $name, ?bool $value, ?Metadata $metadata = null) : BooleanEntry
{
    return bool_entry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function datetime_entry(string $name, \DateTimeInterface|string|null $value, ?Metadata $metadata = null) : DateTimeEntry
{
    return new DateTimeEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function time_entry(string $name, \DateInterval|string|null $value, ?Metadata $metadata = null) : TimeEntry
{
    return new TimeEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function date_entry(string $name, \DateTimeInterface|string|null $value, ?Metadata $metadata = null) : DateEntry
{
    return new DateEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function int_entry(string $name, ?int $value, ?Metadata $metadata = null) : IntegerEntry
{
    return new IntegerEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function integer_entry(string $name, ?int $value, ?Metadata $metadata = null) : IntegerEntry
{
    return int_entry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function enum_entry(string $name, ?\UnitEnum $enum, ?Metadata $metadata = null) : EnumEntry
{
    return new EnumEntry($name, $enum, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function float_entry(string $name, float|int|string|null $value, ?Metadata $metadata = null) : FloatEntry
{
    return new FloatEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_entry(string $name, array|string|null $data, ?Metadata $metadata = null) : JsonEntry
{
    return new JsonEntry($name, $data, $metadata);
}

/**
 * @throws InvalidArgumentException
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_object_entry(string $name, array|string|null $data, ?Metadata $metadata = null) : JsonEntry
{
    if (\is_string($data)) {
        return new JsonEntry($name, $data, $metadata);
    }

    return JsonEntry::object($name, $data, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function str_entry(string $name, ?string $value, ?Metadata $metadata = null) : StringEntry
{
    return new StringEntry($name, $value, $metadata);
}

/**
 * This functions is an alias for creating string entry from null.
 * The main difference between using this function an simply str_entry with second argument null
 * is that this function will also keep a note in the metadata that type might not be final.
 * For example when we need to guess column type from rows because schema was not provided,
 * and given column in the first row is null, it might still change once we get to the second row.
 * That metadata is used to determine if string_entry was created from null or not.
 *
 * By design flow assumes when guessing column type that null would be a string (the most flexible type).
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function null_entry(string $name, ?Metadata $metadata = null) : StringEntry
{
    return StringEntry::fromNull($name, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function string_entry(string $name, ?string $value, ?Metadata $metadata = null) : StringEntry
{
    return str_entry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function uuid_entry(string $name, \Flow\Types\Value\Uuid|string|null $value, ?Metadata $metadata = null) : UuidEntry
{
    return new UuidEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_entry(string $name, \DOMDocument|string|null $value, ?Metadata $metadata = null) : XMLEntry
{
    return new XMLEntry($name, $value, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_element_entry(string $name, \DOMElement|string|null $value, ?Metadata $metadata = null) : XMLElementEntry
{
    return new XMLElementEntry($name, $value, $metadata);
}

/**
 * @param Entry<mixed, mixed> ...$entries
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function entries(Entry ...$entries) : Entries
{
    return new Entries(...$entries);
}

/**
 * @template T of array
 *
 * @param StructureType<T> $type
 * @param ?array<array-key, mixed> $value
 *
 * @return Entry\StructureEntry<T>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function struct_entry(string $name, ?array $value, StructureType $type, ?Metadata $metadata = null) : StructureEntry
{
    return new StructureEntry($name, $value, $type, $metadata);
}

/**
 * @template T of array
 *
 * @param StructureType<T> $type
 * @param ?array<array-key, mixed> $value
 *
 * @return Entry\StructureEntry<T>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function structure_entry(string $name, ?array $value, StructureType $type, ?Metadata $metadata = null) : StructureEntry
{
    return new StructureEntry($name, $value, $type, $metadata);
}

/**
 * @template T of array
 *
 * @param T $elements
 *
 * @return StructureType<T>
 *
 * @deprecated please use \Flow\Types\DSL\type_structure(array $elements) instead
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_structure(array $elements) : StructureType
{
    return type_structure_new($elements);
}

/**
 * @template T
 *
 * @param Type<T> $first
 * @param Type<T> $second
 * @param Type<T> ...$types
 *
 * @return UnionType<T, T>
 *
 * @deprecated please use \Flow\Types\DSL\type_union(Type $first, Type $second, Type ...$types) : UnionType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_union(Type $first, Type $second, Type ...$types) : UnionType
{
    return type_union_new($first, $second, ...$types);
}

/**
 * @template T
 *
 * @param Type<T> $type
 *
 * @return OptionalType<T>
 *
 * @deprecated please use \Flow\Types\DSL\type_optional(Type $type) : OptionalType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_optional(Type $type) : OptionalType
{
    return type_optional_new($type);
}

/**
 * @param array<mixed> $data
 *
 * @return Type<mixed>
 *
 * @deprecated please use \Flow\Types\DSL\type_from_array(array $data) : Type
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_from_array(array $data) : Type
{
    return type_from_array_new($data);
}

/**
 * @param Type<mixed> $type
 *
 * @deprecated please use \Flow\Types\DSL\is_nullable(Type $type) : bool
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function is_nullable(Type $type) : bool
{
    return is_nullable_new($type);
}

/**
 * @template TLeft
 * @template TRight
 *
 * @param Type<TLeft> $left
 * @param Type<TRight> $right
 *
 * @deprecated please use \Flow\Types\DSL\type_equals(Type $left, Type $right) : bool
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_equals(Type $left, Type $right) : bool
{
    return type_equals_new($left, $right);
}

/**
 * @param Type<mixed> ...$types
 *
 * @deprecated please use \Flow\Types\DSL\types(Type ...$types) : Types
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function types(Type ...$types) : Types
{
    return types_new(...$types);
}

/**
 * @template T
 *
 * @param ?list $value
 * @param ListType<T> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function list_entry(string $name, ?array $value, ListType $type, ?Metadata $metadata = null) : ListEntry
{
    return new ListEntry($name, $value, $type, $metadata);
}

/**
 * @template T
 *
 * @param Type<T> $element
 *
 * @return ListType<T>
 *
 * @deprecated please use \Flow\Types\DSL\type_list(Type $element) : ListType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_list(Type $element) : ListType
{
    return type_list_new($element);
}

/**
 * @template T
 *
 * @param Type<T> $value_type
 *
 * @return MapType<array-key, T>
 *
 * @deprecated please use \Flow\Types\DSL\type_map(StringType|IntegerType $key_type, Type $value_type) : MapType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_map(StringType|IntegerType $key_type, Type $value_type) : MapType
{
    return type_map_new($key_type, $value_type);
}

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @param MapType<TKey, TValue> $mapType
 * @param ?array $value
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function map_entry(string $name, ?array $value, MapType $mapType, ?Metadata $metadata = null) : MapEntry
{
    return new MapEntry($name, $value, $mapType, $metadata);
}

/**
 * @deprecated please use \Flow\Types\DSL\type_json() : JsonType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_json() : JsonType
{
    return type_json_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_datetime() : DateTimeType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_datetime() : DateTimeType
{
    return type_datetime_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_date() : DateType
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_date() : DateType
{
    return type_date_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_time() : TimeType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_time() : TimeType
{
    return type_time_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_xml() : XMLType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_xml() : XMLType
{
    return type_xml_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_xml_element() : XMLElementType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_xml_element() : XMLElementType
{
    return type_xml_element_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_uuid() : UuidType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_uuid() : UuidType
{
    return type_uuid_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_integer() : IntegerType
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TYPE)]
function type_int() : IntegerType
{
    return type_integer_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_integer() : IntegerType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_integer() : IntegerType
{
    return type_integer_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_string() : StringType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_string() : StringType
{
    return type_string_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_float() : FloatType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_float() : FloatType
{
    return type_float_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_boolean() : BooleanType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_boolean() : BooleanType
{
    return type_boolean_new();
}

/**
 * @template T of object
 *
 * @param class-string<T> $class
 *
 * @return \Flow\Types\Type\Logical\InstanceOfType<T>
 *
 * @deprecated please use \Flow\Types\DSL\type_instance_of(string $class) : InstanceOfType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_instance_of(string $class) : InstanceOfType
{
    return type_instance_of_new($class);
}

/**
 * @deprecated please use \Flow\Types\DSL\type_resource() : ResourceType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_resource() : ResourceType
{
    return type_resource_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_array() : ArrayType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_array() : ArrayType
{
    return type_array_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_callable() : CallableType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_callable() : CallableType
{
    return type_callable_new();
}

/**
 * @deprecated please use \Flow\Types\DSL\type_null() : NullType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_null() : NullType
{
    return type_null_new();
}

/**
 * @template T of UnitEnum
 *
 * @param class-string<T> $class
 *
 * @return EnumType<T>
 *
 * @deprecated please use \Flow\Types\DSL\type_enum(string $class) : EnumType
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_enum(string $class) : EnumType
{
    return type_enum_new($class);
}

/**
 * @param Entry<mixed, mixed> ...$entry
 */
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
function array_keys_style_convert(ScalarFunction $ref, OldStringStyles|StringStyles|string $style = StringStyles::SNAKE) : ArrayKeysStyleConvert
{
    if ($style instanceof OldStringStyles) {
        $style = StringStyles::fromString($style->value);
    }

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

/**
 * Concat all values. If you want to concatenate values with separator use concat_ws function.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function concat(ScalarFunction|string ...$functions) : Concat
{
    return new Concat(...$functions);
}

/**
 * Concat all values with separator.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function concat_ws(ScalarFunction|string $separator, ScalarFunction|string ...$functions) : ConcatWithSeparator
{
    return new ConcatWithSeparator($separator, ...$functions);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function hash(mixed $value, Algorithm $algorithm = new NativePHPHash()) : Hash
{
    return new Hash($value, $algorithm);
}

/**
 * @param string|Type<mixed> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function cast(mixed $value, string|Type $type) : Cast
{
    return new Cast($value, $type);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function coalesce(ScalarFunction ...$values) : Coalesce
{
    return new Coalesce(...$values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function count(?EntryReference $function = null) : Count
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
 *
 * @return Entry<mixed, mixed>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function to_entry(string $name, mixed $data, EntryFactory $entryFactory = new EntryFactory()) : Entry
{
    return $entryFactory->create($name, $data);
}

/**
 * @param array<array<mixed>>|array<mixed|string> $data
 * @param array<Partition>|Partitions $partitions
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function array_to_row(array $data, EntryFactory $entryFactory = new EntryFactory(), array|Partitions $partitions = [], ?Schema $schema = null) : Row
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
function array_to_rows(array $data, EntryFactory $entryFactory = new EntryFactory(), array|Partitions $partitions = [], ?Schema $schema = null) : Rows
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
function average(EntryReference|string $ref, int $scale = 2, Rounding $rounding = Rounding::HALF_UP) : Average
{
    return new Average(\is_string($ref) ? ref($ref) : $ref, $scale, $rounding);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function greatest(mixed ...$values) : Greatest
{
    return new Greatest($values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function least(mixed ...$values) : Least
{
    return new Least($values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect(EntryReference|string $ref) : Collect
{
    return new Collect(\is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function string_agg(EntryReference|string $ref, string $separator = ', ', ?SortOrder $sort = null) : StringAggregate
{
    return new StringAggregate(\is_string($ref) ? ref($ref) : $ref, $separator, $sort);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect_unique(EntryReference|string $ref) : CollectUnique
{
    return new CollectUnique(\is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function window() : Window
{
    return new Window();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function sum(EntryReference|string $ref) : Sum
{
    return new Sum(\is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function first(EntryReference|string $ref) : First
{
    return new First(\is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function last(EntryReference|string $ref) : Last
{
    return new Last(\is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function max(EntryReference|string $ref) : Max
{
    return new Max(\is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function min(EntryReference|string $ref) : Min
{
    return new Min(\is_string($ref) ? ref($ref) : $ref);
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

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_to_json(Schema $schema, bool $pretty = false) : string
{
    return (new JsonSchemaFormatter($pretty))->format($schema);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_validate(Schema $expected, Schema $given, SchemaValidator $validator = new StrictValidator()) : bool
{
    return $validator->isValid($expected, $given);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_evolving_validator() : EvolvingValidator
{
    return new EvolvingValidator();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_strict_validator() : StrictValidator
{
    return new StrictValidator();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_selective_validator() : SelectiveValidator
{
    return new SelectiveValidator();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_from_json(string $schema) : Schema
{
    return Schema::fromArray(\json_decode($schema, true, 512, JSON_THROW_ON_ERROR));
}

/**
 * @param array<string, array<bool|float|int|string>|bool|float|int|string> $metadata
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_metadata(array $metadata = []) : Metadata
{
    return Metadata::fromArray($metadata);
}

/**
 * Alias for `int_schema`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function int_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return integer_schema($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function integer_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::integer($name, $nullable, $metadata);
}

/**
 * Alias for `string_schema`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function str_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return string_schema($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function string_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::string($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function bool_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::boolean($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function float_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::float($name, $nullable, $metadata);
}

/**
 * @param MapType<array-key, mixed> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function map_schema(string $name, MapType $type, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::map($name, $type, $nullable, $metadata);
}

/**
 * @param ListType<mixed> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function list_schema(string $name, ListType $type, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::list($name, $type, $nullable, $metadata);
}

/**
 * @param class-string<\UnitEnum> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function enum_schema(string $name, string $type, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::enum($name, $type, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function null_schema(string $name, ?Metadata $metadata = null) : Definition
{
    return Definition::string($name, true, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function datetime_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::datetime($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function time_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::time($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function date_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::date($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function json_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::json($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::xml($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_element_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::xml_element($name, $nullable, $metadata);
}

/**
 * @template T of array
 *
 * @param StructureType<T> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function struct_schema(string $name, StructureType $type, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $nullable, $metadata);
}

/**
 * @template T of array
 *
 * @param StructureType<T> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function structure_schema(string $name, StructureType $type, bool $nullable = false, ?Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function uuid_schema(string $name, bool $nullable = false, ?Metadata $metadata = null) : Definition
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

/**
 * @return Type<mixed>
 */
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
    return ($formatter ?? new AsciiTableFormatter())->format($rows, $truncate);
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
function compare_entries_by_name(Order $order = Order::ASC) : Comparator
{
    return new NameComparator($order);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_name_desc() : Comparator
{
    return new NameComparator(Order::DESC);
}

/**
 * @param array<class-string<Entry<mixed, mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type(array $priorities = TypePriorities::PRIORITIES, Order $order = Order::ASC) : Comparator
{
    return new TypeComparator(new TypePriorities($priorities), $order);
}

/**
 * @param array<class-string<Entry<mixed, mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type_desc(array $priorities = TypePriorities::PRIORITIES) : Comparator
{
    return new TypeComparator(new TypePriorities($priorities), Order::DESC);
}

/**
 * @param array<class-string<Entry<mixed, mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type_and_name(array $priorities = TypePriorities::PRIORITIES, Order $order = Order::ASC) : Comparator
{
    return new CombinedComparator(
        new TypeComparator(new TypePriorities($priorities), $order),
        new NameComparator($order)
    );
}

/**
 * @param array<string|Type<mixed>>|Type<mixed> $type
 * @param mixed $value
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function is_type(Type|array $type, mixed $value) : bool
{
    if ($type instanceof Type) {
        $type = [$type];
    }

    foreach ($type as $nextType) {
        if (\is_string($nextType)) {
            if (match (\strtolower($nextType)) {
                'str', 'string' => \is_string($value),
                'int', 'integer' => \is_int($value),
                'float' => \is_float($value),
                'null' => null === $value,
                'object' => \is_object($value),
                'array' => \is_array($value),
                'list' => \is_array($value) && \array_is_list($value),
                default => match (\class_exists($nextType) || \enum_exists($nextType)) {
                    true => $value instanceof $nextType,
                    false => throw new RuntimeException('Unexpected type: ' . $nextType),
                },
            }) {
                return true;
            }
        } else {
            if ($nextType->isValid($value)) {
                return true;
            }
        }
    }

    return false;
}

/**
 * @template T
 *
 * @param Type<T> $type
 * @param class-string<Type<mixed>> $typeClass
 *
 * @deprecated please use \Flow\Types\DSL\type_is($type, $typeClass): bool instead
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_is(Type $type, string $typeClass) : bool
{
    return type_is_new($type, $typeClass);
}

/**
 * @template T
 *
 * @param Type<T> $type
 * @param class-string<Type<mixed>> $typeClass
 * @param class-string<Type<mixed>> ...$typeClasses
 *
 * @deprecated please use \Flow\Types\DSL\type_is_any($type, $typeClass, ...$typeClasses): bool instead
 */
#[DocumentationDSL(module: Module::DEPRECATED, type: DSLType::DEPRECATED)]
function type_is_any(Type $type, string $typeClass, string ...$typeClasses) : bool
{
    return type_is_any_new($type, $typeClass, ...$typeClasses);
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

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function with_entry(string $name, ScalarFunction $function) : WithEntry
{
    return new WithEntry($name, $function);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function constraint_unique(string $reference, string ...$references) : UniqueConstraint
{
    return new UniqueConstraint($reference, ...$references);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function analyze() : Analyze
{
    return new Analyze();
}
