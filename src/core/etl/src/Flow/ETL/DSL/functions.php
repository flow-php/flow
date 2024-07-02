<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\ErrorHandler\{IgnoreError, SkipRows, ThrowError};
use Flow\ETL\Exception\{InvalidArgumentException,
    RuntimeException,
    SchemaDefinitionNotFoundException};
use Flow\ETL\Extractor\LocalFileListExtractor;
use Flow\ETL\Filesystem\{SaveMode};
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\Function\{All, Any, ArrayExists, ArrayGet, ArrayGetCollection, ArrayKeyRename, ArrayKeysStyleConvert, ArrayMerge, ArrayMergeCollection, ArrayReverse, ArraySort, ArrayUnpack, Average, Between, CallMethod, Capitalize, Cast, Collect, CollectUnique, Combine, Concat, Count, DateTimeFormat, DenseRank, Exists, First, Hash, Last, ListFunctions, Literal, Max, Min, Not, Now, NumberFormat, Optional, PregMatch, PregMatchAll, PregReplace, Rank, Round, RowNumber, Sanitize, ScalarFunction, Size, Split, Sprintf, StructureFunctions, Sum, ToDate, ToDateTime, ToLower, ToMoney, ToTimeZone, ToUpper, Ulid, Uuid, When};
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
use Flow\ETL\{Config,
    ConfigBuilder,
    DataFrame,
    Extractor,
    Flow,
    FlowContext,
    Formatter,
    Join\Comparison,
    Join\Comparison\Equal,
    Join\Comparison\Identical,
    Join\Expression,
    Loader,
    Pipeline,
    Row,
    Rows,
    Transformer,
    Window};
use Flow\Filesystem\Stream\Mode;
use Flow\Filesystem\{Partition, Partitions, Path};

/**
 * Alias for data_frame() : Flow.
 */
function df(Config|ConfigBuilder|null $config = null) : Flow
{
    return data_frame($config);
}

function data_frame(Config|ConfigBuilder|null $config = null) : Flow
{
    return new Flow($config);
}

function from_rows(Rows ...$rows) : Extractor\ProcessExtractor
{
    return new Extractor\ProcessExtractor(...$rows);
}

function from_path_partitions(Path|string $path) : Extractor\PathPartitionsExtractor
{
    return new Extractor\PathPartitionsExtractor(\is_string($path) ? Path::realpath($path) : $path);
}

function from_array(iterable $array, ?Schema $schema = null) : Extractor\ArrayExtractor
{
    return new Extractor\ArrayExtractor($array, schema: $schema);
}

function from_cache(string $id, ?Extractor $fallback_extractor = null, bool $clear = false) : Extractor\CacheExtractor
{
    return new Extractor\CacheExtractor($id, $fallback_extractor, $clear);
}

function from_all(Extractor ...$extractors) : Extractor\ChainExtractor
{
    return new Extractor\ChainExtractor(...$extractors);
}

function from_memory(Memory $memory) : Extractor\MemoryExtractor
{
    return new Extractor\MemoryExtractor($memory);
}

function local_files(string|Path $directory, bool $recursive = false) : LocalFileListExtractor
{
    return new LocalFileListExtractor(\is_string($directory) ? Path::realpath($directory) : $directory, $recursive);
}

/**
 * @param int<1, max> $chunk_size
 */
function chunks_from(Extractor $extractor, int $chunk_size) : Extractor\ChunkExtractor
{
    return new Extractor\ChunkExtractor($extractor, $chunk_size);
}

function from_pipeline(Pipeline $pipeline) : Extractor\PipelineExtractor
{
    return new Extractor\PipelineExtractor($pipeline);
}

function from_data_frame(DataFrame $data_frame) : Extractor\DataFrameExtractor
{
    return new Extractor\DataFrameExtractor($data_frame);
}

function from_sequence_date_period(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, \DateTimeInterface $end, int $options = 0) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $end, $options)),
        $entry_name
    );
}

function from_sequence_date_period_recurrences(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, int $recurrences, int $options = 0) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $recurrences, $options)),
        $entry_name
    );
}

function from_sequence_number(string $entry_name, string|int|float $start, string|int|float $end, int|float $step = 1) : Extractor\SequenceExtractor
{
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\NumberSequenceGenerator($start, $end, $step),
        $entry_name
    );
}

function to_callable(callable $callable) : CallbackLoader
{
    return new CallbackLoader($callable);
}

function to_memory(Memory $memory) : MemoryLoader
{
    return new MemoryLoader($memory);
}

function to_output(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::output($truncate, $output, $formatter, $schemaFormatter);
}

function to_stderr(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stderr($truncate, $output, $formatter, $schemaFormatter);
}

function to_stdout(int|bool $truncate = 20, Output $output = Output::rows, Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return StreamLoader::stdout($truncate, $output, $formatter, $schemaFormatter);
}

function to_stream(string $uri, int|bool $truncate = 20, Output $output = Output::rows, string $mode = 'w', Formatter $formatter = new Formatter\AsciiTableFormatter(), SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter()) : StreamLoader
{
    return new StreamLoader($uri, Mode::from($mode), $truncate, $output, $formatter, $schemaFormatter, StreamLoader\Type::custom);
}

function to_transformation(Transformer $transformer, Loader $loader) : TransformerLoader
{
    return new TransformerLoader($transformer, $loader);
}

function to_branch(ScalarFunction $condition, Loader $loader) : Loader
{
    return new Loader\BranchingLoader($condition, $loader);
}

/**
 * @param array<mixed> $data
 */
function array_entry(string $array, ?array $data) : Entry\ArrayEntry
{
    return new Entry\ArrayEntry($array, $data);
}

function bool_entry(string $name, ?bool $value) : Entry\BooleanEntry
{
    return new Entry\BooleanEntry($name, $value);
}

function boolean_entry(string $name, ?bool $value) : Entry\BooleanEntry
{
    return bool_entry($name, $value);
}

function datetime_entry(string $name, \DateTimeInterface|string|null $value) : Entry\DateTimeEntry
{
    return new Entry\DateTimeEntry($name, $value);
}

function int_entry(string $name, ?int $value) : Entry\IntegerEntry
{
    return new Entry\IntegerEntry($name, $value);
}

function integer_entry(string $name, ?int $value) : Entry\IntegerEntry
{
    return int_entry($name, $value);
}

function enum_entry(string $name, ?\UnitEnum $enum) : Entry\EnumEntry
{
    return new Entry\EnumEntry($name, $enum);
}

function float_entry(string $name, ?float $value) : Entry\FloatEntry
{
    return new Entry\FloatEntry($name, $value);
}

function json_entry(string $name, array|string|null $data) : Entry\JsonEntry
{
    return new Entry\JsonEntry($name, $data);
}

/**
 * @throws InvalidArgumentException
 */
function json_object_entry(string $name, array|string|null $data) : Entry\JsonEntry
{
    if (\is_string($data)) {
        return new Entry\JsonEntry($name, $data);
    }

    return Entry\JsonEntry::object($name, $data);
}

function object_entry(string $name, ?object $data) : Entry\ObjectEntry
{
    return new Entry\ObjectEntry($name, $data);
}

function obj_entry(string $name, ?object $data) : Entry\ObjectEntry
{
    return object_entry($name, $data);
}

function str_entry(string $name, ?string $value) : Entry\StringEntry
{
    return new Entry\StringEntry($name, $value);
}

function string_entry(string $name, ?string $value) : Entry\StringEntry
{
    return str_entry($name, $value);
}

function uuid_entry(string $name, \Flow\ETL\PHP\Value\Uuid|string|null $value) : Entry\UuidEntry
{
    return new Entry\UuidEntry($name, $value);
}

function xml_entry(string $name, \DOMDocument|string|null $value) : Entry\XMLEntry
{
    return new Entry\XMLEntry($name, $value);
}

function xml_element_entry(string $name, \DOMElement|string|null $value) : Entry\XMLElementEntry
{
    return new Entry\XMLElementEntry($name, $value);
}

function entries(Entry ...$entries) : Row\Entries
{
    return new Row\Entries(...$entries);
}

function struct_entry(string $name, ?array $value, StructureType $type) : Entry\StructureEntry
{
    return new Entry\StructureEntry($name, $value, $type);
}

function structure_entry(string $name, ?array $value, StructureType $type) : Entry\StructureEntry
{
    return new Entry\StructureEntry($name, $value, $type);
}

/**
 * @param array<StructureElement> $elements
 */
function struct_type(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

/**
 * @param array<StructureElement> $elements
 */
function structure_type(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

/**
 * @param array<StructureElement> $elements
 */
function type_structure(array $elements, bool $nullable = false) : StructureType
{
    return new StructureType($elements, $nullable);
}

function struct_element(string $name, Type $type) : StructureElement
{
    return new StructureElement($name, $type);
}

function structure_element(string $name, Type $type) : StructureElement
{
    return new StructureElement($name, $type);
}

function list_entry(string $name, ?array $value, ListType $type) : Entry\ListEntry
{
    return new Entry\ListEntry($name, $value, $type);
}

function type_list(Type $element, bool $nullable = false) : ListType
{
    return new ListType(new ListElement($element), $nullable);
}

function type_map(ScalarType $key_type, Type $value_type, bool $nullable = false) : MapType
{
    return new MapType(new MapKey($key_type), new MapValue($value_type), $nullable);
}

function map_entry(string $name, ?array $value, MapType $mapType) : Entry\MapEntry
{
    return new Entry\MapEntry($name, $value, $mapType);
}

function type_json(bool $nullable = false) : JsonType
{
    return new JsonType($nullable);
}

function type_datetime(bool $nullable = false) : DateTimeType
{
    return new DateTimeType($nullable);
}

function type_xml(bool $nullable = false) : XMLType
{
    return new XMLType($nullable);
}

function type_xml_element(bool $nullable = false) : XMLElementType
{
    return new XMLElementType($nullable);
}

function type_uuid(bool $nullable = false) : UuidType
{
    return new UuidType($nullable);
}

function type_int(bool $nullable = false) : ScalarType
{
    return ScalarType::integer($nullable);
}

function type_integer(bool $nullable = false) : ScalarType
{
    return ScalarType::integer($nullable);
}

function type_string(bool $nullable = false) : ScalarType
{
    return ScalarType::string($nullable);
}

function type_float(bool $nullable = false) : ScalarType
{
    return ScalarType::float($nullable);
}

function type_boolean(bool $nullable = false) : ScalarType
{
    return ScalarType::boolean($nullable);
}

/**
 * @param class-string $class
 */
function type_object(string $class, bool $nullable = false) : ObjectType
{
    return new ObjectType($class, $nullable);
}

function type_resource(bool $nullable = true) : ResourceType
{
    return new ResourceType($nullable);
}

function type_array(bool $empty = false, bool $nullable = false) : ArrayType
{
    return new ArrayType($empty, $nullable);
}

function type_callable(bool $nullable = true) : CallableType
{
    return new CallableType($nullable);
}

function type_null() : NullType
{
    return new NullType();
}

/**
 * @param class-string<\UnitEnum> $class
 */
function type_enum(string $class, bool $nullable = false) : EnumType
{
    return new EnumType($class, $nullable);
}

function row(Entry ...$entry) : Row
{
    return Row::create(...$entry);
}

function rows(Row ...$row) : Rows
{
    return new Rows(...$row);
}

function rows_partitioned(array $rows, array|Partitions $partitions) : Rows
{
    return Rows::partitioned($rows, $partitions);
}

function col(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

function entry(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

function ref(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

function structure_ref(string $entry) : StructureFunctions
{
    return ref($entry)->structure();
}

function list_ref(string $entry) : ListFunctions
{
    return ref($entry)->list();
}

function refs(string|Reference ...$entries) : References
{
    return new References(...$entries);
}

function optional(ScalarFunction $function) : Optional
{
    return new Optional($function);
}

function lit(mixed $value) : Literal
{
    return new Literal($value);
}

function exists(ScalarFunction $ref) : Exists
{
    return new Exists($ref);
}

function when(ScalarFunction $ref, ScalarFunction $then, ?ScalarFunction $else = null) : When
{
    return new When($ref, $then, $else);
}

function array_get(ScalarFunction $ref, string $path) : ArrayGet
{
    return new ArrayGet($ref, $path);
}

function array_get_collection(ScalarFunction $ref, string ...$keys) : ArrayGetCollection
{
    return new ArrayGetCollection($ref, $keys);
}

function array_get_collection_first(ScalarFunction $ref, string ...$keys) : ArrayGetCollection
{
    return ArrayGetCollection::fromFirst($ref, $keys);
}

function array_exists(ScalarFunction $ref, string $path) : ArrayExists
{
    return new ArrayExists($ref, $path);
}

function array_merge(ScalarFunction $left, ScalarFunction $right) : ArrayMerge
{
    return new ArrayMerge($left, $right);
}

function array_merge_collection(ScalarFunction $ref) : ArrayMergeCollection
{
    return new ArrayMergeCollection($ref);
}

function array_key_rename(ScalarFunction $ref, string $path, string $newName) : ArrayKeyRename
{
    return new ArrayKeyRename($ref, $path, $newName);
}

function array_keys_style_convert(ScalarFunction $ref, StringStyles|string $style = StringStyles::SNAKE) : ArrayKeysStyleConvert
{
    return new ArrayKeysStyleConvert($ref, $style instanceof StringStyles ? $style : StringStyles::fromString($style));
}

function array_sort(ScalarFunction $function, ?string $sort_function = null, ?int $flags = null, bool $recursive = true) : ArraySort
{
    return new ArraySort($function, $sort_function ? Sort::fromString($sort_function) : Sort::sort, $flags, $recursive);
}

function array_reverse(ScalarFunction $function, bool $preserveKeys = false) : ArrayReverse
{
    return new ArrayReverse($function, $preserveKeys);
}

function now(\DateTimeZone $time_zone = new \DateTimeZone('UTC')) : Now
{
    return new Now($time_zone);
}

function between(ScalarFunction $ref, ScalarFunction $lowerBound, ScalarFunction $upperBound, Boundary $boundary = Boundary::LEFT_INCLUSIVE) : Between
{
    return new Between($ref, $lowerBound, $upperBound, $boundary);
}

function to_date_time(ScalarFunction $ref, string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDateTime
{
    return new ToDateTime($ref, $format, $timeZone);
}

function to_date(ScalarFunction $ref, string $format = 'Y-m-d', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDate
{
    return new ToDate($ref, $format, $timeZone);
}

function date_time_format(ScalarFunction $ref, string $format) : DateTimeFormat
{
    return new DateTimeFormat($ref, $format);
}

/**
 * @param non-empty-string $separator
 */
function split(ScalarFunction $ref, string $separator, int $limit = PHP_INT_MAX) : Split
{
    return new Split($ref, $separator, $limit);
}

function combine(ScalarFunction $keys, ScalarFunction $values) : Combine
{
    return new Combine($keys, $values);
}

function concat(ScalarFunction ...$functions) : Concat
{
    return new Concat(...$functions);
}

function hash(ScalarFunction $function, string $algorithm = 'xxh128', bool $binary = false, array $options = []) : Hash
{
    return new Hash($function, $algorithm, $binary, $options);
}

function cast(ScalarFunction $function, string|Type $type) : Cast
{
    return new Cast($function, $type);
}

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
function array_unpack(ScalarFunction $function, array $skip_keys = [], ?string $entry_prefix = null) : ArrayUnpack
{
    return new ArrayUnpack($function, $skip_keys, $entry_prefix);
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
function array_expand(ScalarFunction $function, ArrayExpand $expand = ArrayExpand::VALUES) : \Flow\ETL\Function\ArrayExpand
{
    return new \Flow\ETL\Function\ArrayExpand($function, $expand);
}

function size(ScalarFunction $function) : Size
{
    return new Size($function);
}

function uuid_v4() : Uuid
{
    return Uuid::uuid4();
}

function uuid_v7(?ScalarFunction $function = null) : Uuid
{
    return Uuid::uuid7($function);
}

function ulid(?ScalarFunction $function = null) : Ulid
{
    return new Ulid($function);
}

function lower(ScalarFunction $function) : ToLower
{
    return new ToLower($function);
}

function capitalize(ScalarFunction $function) : Capitalize
{
    return new Capitalize($function);
}

function upper(ScalarFunction $function) : ToUpper
{
    return new ToUpper($function);
}

function call_method(ScalarFunction $object, ScalarFunction $method, ScalarFunction ...$params) : CallMethod
{
    return new CallMethod($object, $method, ...$params);
}

function all(ScalarFunction ...$functions) : All
{
    return new All(...$functions);
}

function any(ScalarFunction ...$functions) : Any
{
    return new Any(...$functions);
}

function not(ScalarFunction $function) : Not
{
    return new Not($function);
}

function to_timezone(ScalarFunction $function, ScalarFunction $timeZone) : ToTimeZone
{
    return new ToTimeZone($function, $timeZone);
}

function ignore_error_handler() : IgnoreError
{
    return new IgnoreError();
}

function skip_rows_handler() : SkipRows
{
    return new SkipRows();
}

function throw_error_handler() : ThrowError
{
    return new ThrowError();
}

function to_money(ScalarFunction $amount, ScalarFunction $currency, ?\Money\MoneyParser $moneyParser = null) : ToMoney
{
    if (null !== $moneyParser) {
        return new ToMoney($amount, $currency, $moneyParser);
    }

    return new ToMoney($amount, $currency);
}

function regex_replace(ScalarFunction $pattern, ScalarFunction $replacement, ScalarFunction $subject) : PregReplace
{
    return new PregReplace($pattern, $replacement, $subject);
}

function regex_match_all(ScalarFunction $pattern, ScalarFunction $subject, ?ScalarFunction $flags = null) : PregMatchAll
{
    return new PregMatchAll($pattern, $subject, $flags);
}

function regex_match(ScalarFunction $pattern, ScalarFunction $subject) : PregMatch
{
    return new PregMatch($pattern, $subject);
}

function sprintf(ScalarFunction $format, ScalarFunction ...$args) : Sprintf
{
    return new Sprintf($format, ...$args);
}

function sanitize(ScalarFunction $function, ?ScalarFunction $placeholder = null, ?ScalarFunction $skipCharacters = null) : Sanitize
{
    return new Sanitize($function, $placeholder ?: new Literal('*'), $skipCharacters ?: new Literal(0));
}

/**
 * @param ScalarFunction $function
 * @param null|ScalarFunction $precision
 * @param int<0, max> $mode
 */
function round(ScalarFunction $function, ?ScalarFunction $precision = null, int $mode = PHP_ROUND_HALF_UP) : Round
{
    return new Round($function, $precision ?? lit(2), $mode);
}

function number_format(ScalarFunction $function, ?ScalarFunction $decimals = null, ?ScalarFunction $decimalSeparator = null, ?ScalarFunction $thousandsSeparator = null) : NumberFormat
{
    if ($decimals === null) {
        $decimals = lit(0);
    }

    if ($decimalSeparator === null) {
        $decimalSeparator = lit('.');
    }

    if ($thousandsSeparator === null) {
        $thousandsSeparator = lit(',');
    }

    return new NumberFormat($function, $decimals, $decimalSeparator, $thousandsSeparator);
}

/**
 * @psalm-suppress PossiblyInvalidIterator
 *
 * @param array<array<mixed>>|array<mixed|string> $data
 * @param array<Partition>|Partitions $partitions
 */
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

        return Rows::partitioned([Row::create(...\array_values($entries))], $partitions);
    }

    $rows = [];

    foreach ($data as $row) {
        $entries = [];

        foreach ($row as $column => $value) {
            $name = \is_int($column) ? 'e' . \str_pad((string) $column, 2, '0', STR_PAD_LEFT) : $column;

            try {
                $entries[$name] = $entryFactory->create(\is_int($column) ? 'e' . \str_pad((string) $column, 2, '0', STR_PAD_LEFT) : $column, $value, $schema);
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

        $rows[] = Row::create(...\array_values($entries));
    }

    return Rows::partitioned($rows, $partitions);
}

function rank() : Rank
{
    return new Rank();
}

function dens_rank() : DenseRank
{
    return dense_rank();
}

function dense_rank() : DenseRank
{
    return new DenseRank();
}

function average(Reference|string $ref) : Average
{
    return new Average(is_string($ref) ? ref($ref) : $ref);
}

function collect(Reference|string $ref) : Collect
{
    return new Collect(is_string($ref) ? ref($ref) : $ref);
}

function collect_unique(Reference|string $ref) : CollectUnique
{
    return new CollectUnique(is_string($ref) ? ref($ref) : $ref);
}

function window() : Window
{
    return new Window();
}

function sum(Reference|string $ref) : Sum
{
    return new Sum(is_string($ref) ? ref($ref) : $ref);
}

function first(Reference|string $ref) : First
{
    return new First(is_string($ref) ? ref($ref) : $ref);
}

function last(Reference|string $ref) : Last
{
    return new Last(is_string($ref) ? ref($ref) : $ref);
}

function max(Reference|string $ref) : Max
{
    return new Max(is_string($ref) ? ref($ref) : $ref);
}

function min(Reference|string $ref) : Min
{
    return new Min(is_string($ref) ? ref($ref) : $ref);
}

function row_number() : RowNumber
{
    return new RowNumber();
}

function schema(Definition ...$definitions) : Schema
{
    return new Schema(...$definitions);
}

function schema_to_json(Schema $schema) : string
{
    return \json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
}

function schema_from_json(string $schema) : Schema
{
    return Schema::fromArray(\json_decode($schema, true, 512, JSON_THROW_ON_ERROR));
}

function schema_strict_matcher() : StrictSchemaMatcher
{
    return new StrictSchemaMatcher();
}

function schema_evolving_matcher() : EvolvingSchemaMatcher
{
    return new EvolvingSchemaMatcher();
}

function int_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::integer($name, $nullable, $metadata);
}

function str_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::string($name, $nullable, $metadata);
}

function bool_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::boolean($name, $nullable, $metadata);
}

function float_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::float($name, $nullable, $metadata);
}

function array_schema(string $name, bool $empty = false, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::array($name, $empty, $nullable, $metadata);
}

function object_schema(string $name, ObjectType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::object($name, $type, $metadata);
}

function map_schema(string $name, MapType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::map($name, $type, $metadata);
}

function list_schema(string $name, ListType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::list($name, $type, $metadata);
}

/**
 * @param class-string<\UnitEnum> $type
 */
function enum_schema(string $name, string $type, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::enum($name, $type, $nullable, $metadata);
}

function null_schema(string $name, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::string($name, true, $metadata);
}

function datetime_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::datetime($name, $nullable, $metadata);
}

function json_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::json($name, $nullable, $metadata);
}

function xml_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::xml($name, $nullable, $metadata);
}

function xml_element_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::xml_element($name, $nullable, $metadata);
}

function struct_schema(string $name, StructureType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $metadata);
}

function structure_schema(string $name, StructureType $type, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::structure($name, $type, $metadata);
}

function uuid_schema(string $name, bool $nullable = false, ?Schema\Metadata $metadata = null) : Definition
{
    return Definition::uuid($name, $nullable, $metadata);
}

function execution_context(?Config $config = null) : FlowContext
{
    return new FlowContext($config ?? Config::default());
}

function flow_context(?Config $config = null) : FlowContext
{
    return execution_context($config);
}

function config() : Config
{
    return Config::default();
}

function config_builder() : ConfigBuilder
{
    return new ConfigBuilder();
}

function overwrite() : SaveMode
{
    return SaveMode::Overwrite;
}

function ignore() : SaveMode
{
    return SaveMode::Ignore;
}

function exception_if_exists() : SaveMode
{
    return SaveMode::ExceptionIfExists;
}

function append() : SaveMode
{
    return SaveMode::Append;
}

function get_type(mixed $value) : Type
{
    return (new TypeDetector())->detectType($value);
}

function print_schema(Schema $schema, ?SchemaFormatter $formatter = null) : string
{
    return ($formatter ?? new ASCIISchemaFormatter())->format($schema);
}

function print_rows(Rows $rows, int|bool $truncate = false, ?Formatter $formatter = null) : string
{
    return ($formatter ?? new Formatter\AsciiTableFormatter())->format($rows, $truncate);
}

function identical(Reference|string $left, Reference|string $right) : Identical
{
    return new Identical($left, $right);
}

function equal(Reference|string $left, Reference|string $right) : Equal
{
    return new Equal($left, $right);
}

function compare_all(Comparison ...$comparisons) : Comparison\All
{
    return new Comparison\All(...$comparisons);
}

function compare_any(Comparison ...$comparisons) : Comparison\Any
{
    return new Comparison\Any(...$comparisons);
}

function join_on(array|Comparison $comparisons, string $joinPrefix = '') : Expression
{
    return Expression::on($comparisons, $joinPrefix);
}

function compare_entries_by_name(Transformer\OrderEntries\Order $order = Transformer\OrderEntries\Order::ASC) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\NameComparator($order);
}

function compare_entries_by_name_desc() : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\NameComparator(Transformer\OrderEntries\Order::DESC);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
function compare_entries_by_type(array $priorities = Transformer\OrderEntries\TypePriorities::PRIORITIES, Transformer\OrderEntries\Order $order = Transformer\OrderEntries\Order::ASC) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\TypeComparator(new Transformer\OrderEntries\TypePriorities($priorities), $order);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
function compare_entries_by_type_desc(array $priorities = Transformer\OrderEntries\TypePriorities::PRIORITIES) : Transformer\OrderEntries\Comparator
{
    return new Transformer\OrderEntries\TypeComparator(new Transformer\OrderEntries\TypePriorities($priorities), Transformer\OrderEntries\Order::DESC);
}

/**
 * @param array<class-string<Entry>, int> $priorities
 */
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
