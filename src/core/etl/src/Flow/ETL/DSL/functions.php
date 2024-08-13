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
use Flow\ETL\Function\{
    All,
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
    CallMethod,
    Capitalize,
    Cast,
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
    When
};
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Loader\{CallbackLoader, MemoryLoader, StreamLoader, TransformerLoader};
use Flow\ETL\Memory\Memory;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Map\{MapKey, MapValue};
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{DateTimeType,
    JsonType,
    ListType,
    MapType,
    StructureType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\ETL\PHP\Type\Native\{ArrayType, CallableType, EnumType, NullType, ObjectType, ResourceType, ScalarType};
use Flow\ETL\PHP\Type\{Type, TypeDetector};
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Schema\{Definition, Matcher\EvolvingSchemaMatcher, Matcher\StrictSchemaMatcher, SchemaFormatter};
use Flow\ETL\Row\{Entry, EntryFactory, EntryReference, Reference, References, Schema};
use Flow\ETL\{Attribute\DSL,
    Attribute\Module,
    Attribute\Type as DSLType,
    Cache\Implementation\FilesystemCache,
    Config,
    Config\ConfigBuilder,
    DataFrame,
    Extractor,
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
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function df(Config|ConfigBuilder|null $config = null) : Flow
{
    return data_frame($config);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function data_frame(Config|ConfigBuilder|null $config = null) : Flow
{
    return new Flow($config);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_rows(Rows ...$rows) : Extractor\RowsExtractor
{
    return new Extractor\RowsExtractor(...$rows);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_path_partitions(Path|string $path) : Extractor\PathPartitionsExtractor
{
    return new Extractor\PathPartitionsExtractor(\is_string($path) ? \Flow\Filesystem\DSL\path($path) : $path);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_array(iterable $array, ?Schema $schema = null) : Extractor\ArrayExtractor
{
    return new Extractor\ArrayExtractor($array, schema: $schema);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_cache(string $id, ?Extractor $fallback_extractor = null, bool $clear = false) : Extractor\CacheExtractor
{
    return new Extractor\CacheExtractor($id, $fallback_extractor, $clear);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_all(Extractor ...$extractors) : Extractor\ChainExtractor
{
    return new Extractor\ChainExtractor(...$extractors);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_memory(Memory $memory) : Extractor\MemoryExtractor
{
    return new Extractor\MemoryExtractor($memory);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function files(string|Path $directory) : FilesExtractor
{
    return new FilesExtractor(\is_string($directory) ? \Flow\Filesystem\DSL\path($directory) : $directory);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function filesystem_cache(Path|string|null $cache_dir = null, Filesystem $filesystem = new NativeLocalFilesystem(), Serializer $serializer = new NativePHPSerializer()) : FilesystemCache
{
    return new FilesystemCache($filesystem, $serializer, \is_string($cache_dir) ? Path::realpath($cache_dir) : $cache_dir);
}

/**
 * @param int<1, max> $chunk_size
 */
#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function chunks_from(Extractor $extractor, int $chunk_size) : Extractor\ChunkExtractor
{
    return new Extractor\ChunkExtractor($extractor, $chunk_size);
}
#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_pipeline(Pipeline $pipeline) : Extractor\PipelineExtractor
{
    return new Extractor\PipelineExtractor($pipeline);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_data_frame(DataFrame $data_frame) : Extractor\DataFrameExtractor
{
    return new Extractor\DataFrameExtractor($data_frame);
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, \DateTimeInterface $end, int $options = 0) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $end, $options)),
        $entry_name
    );
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period_recurrences(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, int $recurrences, int $options = 0) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $recurrences, $options)),
        $entry_name
    );
}

#[DSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_number(string $entry_name, string|int|float $start, string|int|float $end, int|float $step = 1) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\NumberSequenceGenerator($start, $end, $step),
        $entry_name
    );
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_callable(callable $callable) : CallbackLoader
{
    return new CallbackLoader($callable);
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_memory(Memory $memory) : MemoryLoader
{
    return new MemoryLoader($memory);
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_output(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::output($truncate, $output, $formatter, $schemaFormatter);
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stderr(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stderr($truncate, $output, $formatter, $schemaFormatter);
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stdout(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stdout($truncate, $output, $formatter, $schemaFormatter);
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stream(string $uri, int|bool $truncate = 20, Output $output = Output::rows, string $mode = 'w', Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return new StreamLoader($uri, Mode::from($mode), $truncate, $output, $formatter, $schemaFormatter, StreamLoader\Type::custom);
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_transformation(Transformer $transformer, Loader $loader) : TransformerLoader
{
    return new TransformerLoader($transformer, $loader);
}

#[DSL(module: Module::CORE, type: DSLType::LOADER)]
function to_branch(ScalarFunction $condition, Loader $loader) : Loader
{
    return new Loader\BranchingLoader($condition, $loader);
}

/**
 * @param array<mixed> $data
 */
#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function array_entry(string $array, ?array $data) : Entry\ArrayEntry
{
    return new Entry\ArrayEntry($array, $data);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function bool_entry(string $name, ?bool $value) : Entry\BooleanEntry
{
    return new Entry\BooleanEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function boolean_entry(string $name, ?bool $value) : Entry\BooleanEntry
{
    return bool_entry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function datetime_entry(string $name, \DateTimeInterface|string|null $value) : Entry\DateTimeEntry
{
    return new Entry\DateTimeEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function int_entry(string $name, ?int $value) : Entry\IntegerEntry
{
    return new Entry\IntegerEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function integer_entry(string $name, ?int $value) : Entry\IntegerEntry
{
    return int_entry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function enum_entry(string $name, ?\UnitEnum $enum) : Entry\EnumEntry
{
    return new Entry\EnumEntry($name, $enum);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function float_entry(string $name, ?float $value) : Entry\FloatEntry
{
    return new Entry\FloatEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_entry(string $name, array|string|null $data) : Entry\JsonEntry
{
    return new Entry\JsonEntry($name, $data);
}

/**
 * @throws InvalidArgumentException
 */
#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_object_entry(string $name, array|string|null $data) : Entry\JsonEntry
{
    if (\is_string($data)) {
        return new Entry\JsonEntry($name, $data);
    }

    return Entry\JsonEntry::object($name, $data);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function object_entry(string $name, ?object $data) : Entry\ObjectEntry
{
    return new Entry\ObjectEntry($name, $data);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function obj_entry(string $name, ?object $data) : Entry\ObjectEntry
{
    return object_entry($name, $data);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function str_entry(string $name, ?string $value) : Entry\StringEntry
{
    return new Entry\StringEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function string_entry(string $name, ?string $value) : Entry\StringEntry
{
    return str_entry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function uuid_entry(string $name, \Flow\ETL\PHP\Value\Uuid|string|null $value) : Entry\UuidEntry
{
    return new Entry\UuidEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_entry(string $name, \DOMDocument|string|null $value) : Entry\XMLEntry
{
    return new Entry\XMLEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_element_entry(string $name, \DOMElement|string|null $value) : Entry\XMLElementEntry
{
    return new Entry\XMLElementEntry($name, $value);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function entries(Entry ...$entries) : Row\Entries
{
    return new Row\Entries(...$entries);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function struct_entry(string $name, ?array $value, StructureType $type) : Entry\StructureEntry
{
    return new Entry\StructureEntry($name, $value, $type);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function structure_entry(string $name, ?array $value, StructureType $type) : Entry\StructureEntry
{
    return new Entry\StructureEntry($name, $value, $type);
}

/**
 * @param array<StructureElement> $elements
 */
#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function struct_type(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

/**
 * @param array<StructureElement> $elements
 */
#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function structure_type(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

/**
 * @param array<StructureElement> $elements
 */
#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_structure(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function struct_element(string $name, Type $type) : StructureElement
{
    return new StructureElement($name, $type);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function structure_element(string $name, Type $type) : StructureElement
{
    return new StructureElement($name, $type);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function list_entry(string $name, ?array $value, ListType $type) : Entry\ListEntry
{
    return new Entry\ListEntry($name, $value, $type);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_list(Type $element, bool $nullable = false) : ListType
{
    return new ListType(new ListElement($element), $nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_map(ScalarType $key_type, Type $value_type, bool $nullable = false) : MapType
{
    return new MapType(new MapKey($key_type), new MapValue($value_type), $nullable);
}

#[DSL(module: Module::CORE, type: DSLType::ENTRY)]
function map_entry(string $name, ?array $value, MapType $mapType) : Entry\MapEntry
{
    return new Entry\MapEntry($name, $value, $mapType);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_json(bool $nullable = false) : JsonType
{
    return new JsonType($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_datetime(bool $nullable = false) : DateTimeType
{
    return new DateTimeType($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_xml(bool $nullable = false) : XMLType
{
    return new XMLType($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_xml_element(bool $nullable = false) : XMLElementType
{
    return new XMLElementType($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_uuid(bool $nullable = false) : UuidType
{
    return new UuidType($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_int(bool $nullable = false) : ScalarType
{
    return ScalarType::integer($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_integer(bool $nullable = false) : ScalarType
{
    return ScalarType::integer($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_string(bool $nullable = false) : ScalarType
{
    return ScalarType::string($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_float(bool $nullable = false) : ScalarType
{
    return ScalarType::float($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_boolean(bool $nullable = false) : ScalarType
{
    return ScalarType::boolean($nullable);
}

/**
 * @param class-string $class
 */
#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_object(string $class, bool $nullable = false) : ObjectType
{
    return new ObjectType($class, $nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_resource(bool $nullable = true) : ResourceType
{
    return new ResourceType($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_array(bool $empty = false, bool $nullable = false) : ArrayType
{
    return new ArrayType($empty, $nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_callable(bool $nullable = true) : CallableType
{
    return new CallableType($nullable);
}

#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_null() : NullType
{
    return new NullType();
}

/**
 * @param class-string<\UnitEnum> $class
 */
#[DSL(module: Module::CORE, type: DSLType::TYPE)]
function type_enum(string $class, bool $nullable = false) : EnumType
{
    return new EnumType($class, $nullable);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function row(Entry ...$entry) : Row
{
    return Row::create(...$entry);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function rows(Row ...$row) : Rows
{
    return new Rows(...$row);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function rows_partitioned(array $rows, array|Partitions $partitions) : Rows
{
    return Rows::partitioned($rows, $partitions);
}

/**
 * An alias for `ref`.
 */
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function col(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

/**
 * An alias for `ref`.
 */
#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function entry(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function ref(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure_ref(string $entry) : StructureFunctions
{
    return ref($entry)->structure();
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function list_ref(string $entry) : ListFunctions
{
    return ref($entry)->list();
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function refs(string|Reference ...$entries) : References
{
    return new References(...$entries);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function optional(ScalarFunction $function) : Optional
{
    return new Optional($function);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function lit(mixed $value) : Literal
{
    return new Literal($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function exists(ScalarFunction $ref) : Exists
{
    return new Exists($ref);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function when(mixed $condition, mixed $then, mixed $else = null) : When
{
    return new When($condition, $then, $else);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get(ScalarFunction $ref, ScalarFunction|string $path) : ArrayGet
{
    return new ArrayGet($ref, $path);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get_collection(ScalarFunction $ref, ScalarFunction|array $keys) : ArrayGetCollection
{
    return new ArrayGetCollection($ref, $keys);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get_collection_first(ScalarFunction $ref, string ...$keys) : ArrayGetCollection
{
    return ArrayGetCollection::fromFirst($ref, $keys);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_exists(ScalarFunction|array $ref, ScalarFunction|string $path) : ArrayPathExists
{
    return new ArrayPathExists($ref, $path);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_merge(ScalarFunction|array $left, ScalarFunction|array $right) : ArrayMerge
{
    return new ArrayMerge($left, $right);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_merge_collection(ScalarFunction|array $array) : ArrayMergeCollection
{
    return new ArrayMergeCollection($array);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_key_rename(ScalarFunction $ref, ScalarFunction|string $path, ScalarFunction|string $newName) : ArrayKeyRename
{
    return new ArrayKeyRename($ref, $path, $newName);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_keys_style_convert(ScalarFunction $ref, StringStyles|string $style = StringStyles::SNAKE) : ArrayKeysStyleConvert
{
    return new ArrayKeysStyleConvert($ref, $style instanceof StringStyles ? $style : StringStyles::fromString($style));
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_sort(ScalarFunction $function, ScalarFunction|Sort|null $sort_function = null, ScalarFunction|int|null $flags = null, ScalarFunction|bool $recursive = true) : ArraySort
{
    if ($sort_function === null) {
        $sort_function = Sort::sort;
    }

    return new ArraySort($function, $sort_function, $flags, $recursive);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_reverse(ScalarFunction|array $function, ScalarFunction|bool $preserveKeys = false) : ArrayReverse
{
    return new ArrayReverse($function, $preserveKeys);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function now(\DateTimeZone|ScalarFunction $time_zone = new \DateTimeZone('UTC')) : Now
{
    return new Now($time_zone);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function between(mixed $value, mixed $lower_bound, mixed $upper_bound, ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE) : Between
{
    return new Between($value, $lower_bound, $upper_bound, $boundary);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_date_time(mixed $ref, ScalarFunction|string $format = 'Y-m-d H:i:s', ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDateTime
{
    return new ToDateTime($ref, $format, $timeZone);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_date(mixed $ref, ScalarFunction|string $format = 'Y-m-d', ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDate
{
    return new ToDate($ref, $format, $timeZone);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function date_time_format(ScalarFunction $ref, string $format) : DateTimeFormat
{
    return new DateTimeFormat($ref, $format);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function split(ScalarFunction|string $value, ScalarFunction|string $separator, ScalarFunction|int $limit = PHP_INT_MAX) : Split
{
    return new Split($value, $separator, $limit);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function combine(ScalarFunction|array $keys, ScalarFunction|array $values) : Combine
{
    return new Combine($keys, $values);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function concat(ScalarFunction|string ...$functions) : Concat
{
    return new Concat(...$functions);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function hash(mixed $value, Algorithm $algorithm = new NativePHPHash()) : Hash
{
    return new Hash($value, $algorithm);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function cast(mixed $value, ScalarFunction|string|Type $type) : Cast
{
    return new Cast($value, $type);
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function count(Reference $function) : Count
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
#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
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
#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_expand(ScalarFunction $function, ArrayExpand $expand = ArrayExpand::VALUES) : \Flow\ETL\Function\ArrayExpand
{
    return new \Flow\ETL\Function\ArrayExpand($function, $expand);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function size(mixed $value) : Size
{
    return new Size($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function uuid_v4() : Uuid
{
    return Uuid::uuid4();
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function uuid_v7(ScalarFunction|\DateTimeInterface|null $value = null) : Uuid
{
    return Uuid::uuid7($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function ulid(ScalarFunction|string|null $value = null) : Ulid
{
    return new Ulid($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function lower(ScalarFunction|string $value) : ToLower
{
    return new ToLower($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function capitalize(ScalarFunction|string $value) : Capitalize
{
    return new Capitalize($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function upper(ScalarFunction|string $value) : ToUpper
{
    return new ToUpper($value);
}

/**
 * @param array<mixed> $params
 */
#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function call_method(object $object, ScalarFunction|string $method, array $params = []) : CallMethod
{
    return new CallMethod($object, $method, $params);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function all(ScalarFunction ...$functions) : All
{
    return new All(...$functions);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function any(ScalarFunction ...$values) : Any
{
    return new Any(...$values);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function not(ScalarFunction $value) : Not
{
    return new Not($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_timezone(ScalarFunction|\DateTimeInterface $value, ScalarFunction|\DateTimeZone|string $timeZone) : ToTimeZone
{
    return new ToTimeZone($value, $timeZone);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function ignore_error_handler() : IgnoreError
{
    return new IgnoreError();
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function skip_rows_handler() : SkipRows
{
    return new SkipRows();
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function throw_error_handler() : ThrowError
{
    return new ThrowError();
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_replace(ScalarFunction|string $pattern, ScalarFunction|string $replacement, ScalarFunction|string $subject, ScalarFunction|int|null $limit = null) : RegexReplace
{
    return new RegexReplace($pattern, $replacement, $subject, $limit);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_match_all(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexMatchAll
{
    return new RegexMatchAll($pattern, $subject, $flags, $offset);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_match(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexMatch
{
    return new RegexMatch($pattern, $subject, $flags, $offset);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : Regex
{
    return new Regex($pattern, $subject, $flags, $offset);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_all(ScalarFunction|string $pattern, ScalarFunction|string $subject, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexAll
{
    return new RegexAll($pattern, $subject, $flags, $offset);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function sprintf(ScalarFunction|string $format, ScalarFunction|float|int|string|null ...$args) : Sprintf
{
    return new Sprintf($format, ...$args);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function sanitize(ScalarFunction|string $value, ScalarFunction|string $placeholder = '*', ScalarFunction|int|null $skipCharacters = null) : Sanitize
{
    return new Sanitize($value, $placeholder, $skipCharacters);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function round(ScalarFunction|int|float $value, ScalarFunction|int $precision = 2, ScalarFunction|int $mode = PHP_ROUND_HALF_UP) : Round
{
    return new Round($value, $precision, $mode);
}

#[DSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function number_format(ScalarFunction|int|float $value, ScalarFunction|int $decimals = 2, ScalarFunction|string $decimal_separator = '.', ScalarFunction|string $thousands_separator = ',') : NumberFormat
{
    return new NumberFormat($value, $decimals, $decimal_separator, $thousands_separator);
}

/**
 * @param array<array<mixed>>|array<mixed|string> $data
 * @param array<Partition>|Partitions $partitions
 */
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function array_to_row(array $data, EntryFactory $entryFactory = new NativeEntryFactory(), array|Partitions $partitions = [], ?Schema $schema = null) : Row
{
    foreach ($data as $key => $v) {
        if (!\is_string($key)) {
            throw new InvalidArgumentException('Passed array keys must be a string. Maybe consider using "array_to_rows()" function?');
        }
    }

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
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
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

#[DSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function rank() : Rank
{
    return new Rank();
}

#[DSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function dens_rank() : DenseRank
{
    return dense_rank();
}

#[DSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function dense_rank() : DenseRank
{
    return new DenseRank();
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function average(Reference|string $ref) : Average
{
    return new Average(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect(Reference|string $ref) : Collect
{
    return new Collect(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect_unique(Reference|string $ref) : CollectUnique
{
    return new CollectUnique(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function window() : Window
{
    return new Window();
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function sum(Reference|string $ref) : Sum
{
    return new Sum(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function first(Reference|string $ref) : First
{
    return new First(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function last(Reference|string $ref) : Last
{
    return new Last(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function max(Reference|string $ref) : Max
{
    return new Max(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function min(Reference|string $ref) : Min
{
    return new Min(is_string($ref) ? ref($ref) : $ref);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function row_number() : RowNumber
{
    return new RowNumber();
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema(Definition ...$definitions) : Schema
{
    return new Schema(...$definitions);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_to_json(Schema $schema) : string
{
    return \json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_from_json(string $schema) : Schema
{
    return Schema::fromArray(\json_decode($schema, true, 512, JSON_THROW_ON_ERROR));
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_strict_matcher() : StrictSchemaMatcher
{
    return new StrictSchemaMatcher();
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_evolving_matcher() : EvolvingSchemaMatcher
{
    return new EvolvingSchemaMatcher();
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function int_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::integer($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function str_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::string($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function bool_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::boolean($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function float_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::float($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function array_schema(string $name, bool $empty = false, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::array($name, $empty, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function object_schema(string $name, ObjectType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::object($name, $type, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function map_schema(string $name, MapType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::map($name, $type, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function list_schema(string $name, ListType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::list($name, $type, $metadata);
}

/**
 * @param class-string<\UnitEnum> $type
 */
#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function enum_schema(string $name, string $type, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::enum($name, $type, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function null_schema(string $name, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::string($name, true, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function datetime_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::datetime($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function json_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::json($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::xml($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_element_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::xml_element($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function struct_schema(string $name, StructureType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function structure_schema(string $name, StructureType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function uuid_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::uuid($name, $nullable, $metadata);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function execution_context(?Config $config = null) : FlowContext
{
    return new FlowContext($config ?? Config::default());
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function flow_context(?Config $config = null) : FlowContext
{
    return execution_context($config);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config() : Config
{
    return Config::default();
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config_builder() : ConfigBuilder
{
    return new ConfigBuilder();
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function overwrite() : SaveMode
{
    return SaveMode::Overwrite;
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function ignore() : SaveMode
{
    return SaveMode::Ignore;
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function exception_if_exists() : SaveMode
{
    return SaveMode::ExceptionIfExists;
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function append() : SaveMode
{
    return SaveMode::Append;
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function get_type(mixed $value) : Type
{
    return (new TypeDetector())->detectType($value);
}

#[DSL(module: Module::CORE, type: DSLType::SCHEMA)]
function print_schema(Schema $schema, ?SchemaFormatter $formatter = null) : string
{
    return ($formatter ?? new ASCIISchemaFormatter())->format($schema);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function print_rows(Rows $rows, int|bool $truncate = false, ?Formatter $formatter = null) : string
{
    return ($formatter ?? new Formatter\AsciiTableFormatter())->format($rows, $truncate);
}

#[DSL(module: Module::CORE, type: DSLType::COMPARISON)]
function identical(Reference|string $left, Reference|string $right) : Identical
{
    return new Identical($left, $right);
}

#[DSL(module: Module::CORE, type: DSLType::COMPARISON)]
function equal(Reference|string $left, Reference|string $right) : Equal
{
    return new Equal($left, $right);
}

#[DSL(module: Module::CORE, type: DSLType::COMPARISON)]
function compare_all(Comparison ...$comparisons) : Comparison\All
{
    return new Comparison\All(...$comparisons);
}

#[DSL(module: Module::CORE, type: DSLType::COMPARISON)]
function compare_any(Comparison ...$comparisons) : Comparison\Any
{
    return new Comparison\Any(...$comparisons);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function join_on(array|Comparison $comparisons, string $joinPrefix = '') : Expression
{
    return Expression::on($comparisons, $joinPrefix);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_name(Transformer\OrderEntries\Order $order = Transformer\OrderEntries\Order::ASC) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\NameComparator($order);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_name_desc() : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\NameComparator(Transformer\OrderEntries\Order::DESC);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type(array $priorities = Transformer\OrderEntries\TypePriorities::PRIORITIES, Transformer\OrderEntries\Order $order = Transformer\OrderEntries\Order::ASC) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\TypeComparator(new Transformer\OrderEntries\TypePriorities($priorities), $order);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type_desc(array $priorities = Transformer\OrderEntries\TypePriorities::PRIORITIES) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\TypeComparator(new Transformer\OrderEntries\TypePriorities($priorities), Transformer\OrderEntries\Order::DESC);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
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
#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
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
                    false => throw new RuntimeException('Unexpected type: ' . $type)
                }
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

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function generate_random_string(int $length = 32, NativePHPRandomValueGenerator $generator = new NativePHPRandomValueGenerator()) : string
{
    return $generator->string($length);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function generate_random_int(int $start = PHP_INT_MIN, int $end = PHP_INT_MAX, NativePHPRandomValueGenerator $generator = new NativePHPRandomValueGenerator()) : int
{
    return $generator->int($start, $end);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function random_string(
    int|ScalarFunction $length,
    RandomValueGenerator $generator = new NativePHPRandomValueGenerator()
) : RandomString {
    return new RandomString($length, $generator);
}

#[DSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function dom_element_to_string(\DOMElement $element, bool $format_output = false, bool $preserver_white_space = false) : string|false
{
    $doc = new \DOMDocument('1.0', 'UTF-8');
    $doc->formatOutput = $format_output;
    $doc->preserveWhiteSpace = $preserver_white_space;

    $importedNode = $doc->importNode($element, true);
    $doc->appendChild($importedNode);

    return $doc->saveXML($doc->documentElement);
}
