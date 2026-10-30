<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use DateInterval;
use DatePeriod;
use DateTimeInterface;
use DateTimeZone;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\DocumentationExample;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\ETL\API\Branch;
use Flow\ETL\API\Config\Bucketing\BucketingConfigBuilder;
use Flow\ETL\API\Config\ConfigBuilder;
use Flow\ETL\API\Config\Join\HashJoinBuilder;
use Flow\ETL\API\Config\Sort\ExternalSortBuilder;
use Flow\ETL\API\Config\Sort\MemorySortBuilder;
use Flow\ETL\API\FilteredBranch;
use Flow\ETL\API\Transactional;
use Flow\ETL\API\Transformation;
use Flow\ETL\API\Transformation\AddRowIndex;
use Flow\ETL\API\Transformation\BatchBy;
use Flow\ETL\API\Transformation\BatchSize;
use Flow\ETL\API\Transformation\Drop;
use Flow\ETL\API\Transformation\Limit;
use Flow\ETL\API\Transformation\MaskColumns;
use Flow\ETL\API\Transformation\Select;
use Flow\ETL\API\TransformedBranch;
use Flow\ETL\Flow;
use Flow\ETL\Foundation\Calculator\Rounding;
use Flow\ETL\Foundation\Clock\SystemClock;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\Functions\All;
use Flow\ETL\Functions\Any;
use Flow\ETL\Functions\ArrayExpand as ArrayExpandFunction;
use Flow\ETL\Functions\ArrayExpand\ArrayExpand;
use Flow\ETL\Functions\ArrayGet;
use Flow\ETL\Functions\ArrayGetCollection;
use Flow\ETL\Functions\ArrayKeyRename;
use Flow\ETL\Functions\ArrayKeysStyleConvert;
use Flow\ETL\Functions\ArrayMerge;
use Flow\ETL\Functions\ArrayMergeCollection;
use Flow\ETL\Functions\ArrayPathExists;
use Flow\ETL\Functions\ArrayReverse;
use Flow\ETL\Functions\ArraySort;
use Flow\ETL\Functions\ArraySort\Sort;
use Flow\ETL\Functions\ArrayUnpack;
use Flow\ETL\Functions\Average;
use Flow\ETL\Functions\Between;
use Flow\ETL\Functions\Between\Boundary;
use Flow\ETL\Functions\CallUserFunc;
use Flow\ETL\Functions\Capitalize;
use Flow\ETL\Functions\Cast;
use Flow\ETL\Functions\Coalesce;
use Flow\ETL\Functions\Collect;
use Flow\ETL\Functions\CollectUnique;
use Flow\ETL\Functions\Combine;
use Flow\ETL\Functions\Concat;
use Flow\ETL\Functions\ConcatWithSeparator;
use Flow\ETL\Functions\Count;
use Flow\ETL\Functions\DateTimeFormat;
use Flow\ETL\Functions\DenseRank;
use Flow\ETL\Functions\EnumName;
use Flow\ETL\Functions\EnumValue;
use Flow\ETL\Functions\Exists;
use Flow\ETL\Functions\First;
use Flow\ETL\Functions\Greatest;
use Flow\ETL\Functions\Hash;
use Flow\ETL\Functions\Join\Condition;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Functions\Join\Condition\Identical;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\Functions\Last;
use Flow\ETL\Functions\Least;
use Flow\ETL\Functions\ListFunctions;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Functions\MatchCases;
use Flow\ETL\Functions\MatchCases\MatchCondition;
use Flow\ETL\Functions\Max;
use Flow\ETL\Functions\Min;
use Flow\ETL\Functions\Not;
use Flow\ETL\Functions\Now;
use Flow\ETL\Functions\NumberFormat;
use Flow\ETL\Functions\Optional;
use Flow\ETL\Functions\RandomString;
use Flow\ETL\Functions\Rank;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\Regex;
use Flow\ETL\Functions\RegexAll;
use Flow\ETL\Functions\RegexMatch;
use Flow\ETL\Functions\RegexMatchAll;
use Flow\ETL\Functions\RegexReplace;
use Flow\ETL\Functions\Round;
use Flow\ETL\Functions\RowNumber;
use Flow\ETL\Functions\Sanitize;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\Size;
use Flow\ETL\Functions\Split;
use Flow\ETL\Functions\Sprintf;
use Flow\ETL\Functions\String\StringStyles;
use Flow\ETL\Functions\StringAggregate;
use Flow\ETL\Functions\Structure;
use Flow\ETL\Functions\StructureFunctions;
use Flow\ETL\Functions\Sum;
use Flow\ETL\Functions\ToDate;
use Flow\ETL\Functions\ToDateTime;
use Flow\ETL\Functions\ToLower;
use Flow\ETL\Functions\ToTimeZone;
use Flow\ETL\Functions\ToUpper;
use Flow\ETL\Functions\Ulid;
use Flow\ETL\Functions\Uuid;
use Flow\ETL\Functions\When;
use Flow\ETL\Functions\Window;
use Flow\ETL\Functions\Window\FrameBound;
use Flow\ETL\Functions\Window\FrameBoundType;
use Flow\ETL\Functions\WithColumn;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\ArrayExtractor;
use Flow\ETL\IO\Extractor\CacheExtractor;
use Flow\ETL\IO\Extractor\ChainExtractor;
use Flow\ETL\IO\Extractor\File\FileMetadataRow;
use Flow\ETL\IO\Extractor\File\PartitionTypes;
use Flow\ETL\IO\Extractor\File\PathPartitionsRow;
use Flow\ETL\IO\Extractor\FileListingExtractor;
use Flow\ETL\IO\Extractor\RowsExtractor;
use Flow\ETL\IO\Extractor\SequenceExtractor;
use Flow\ETL\IO\Extractor\SequenceGenerator\DatePeriodSequenceGenerator;
use Flow\ETL\IO\Extractor\SequenceGenerator\NumberSequenceGenerator;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Formatter;
use Flow\ETL\IO\Formatter\AsciiTableFormatter;
use Flow\ETL\IO\Formatter\Row\ASCIISchemaFormatter;
use Flow\ETL\IO\Formatter\Schema\JsonSchemaFormatter;
use Flow\ETL\IO\Formatter\Schema\PHPFormatter\TypeFormatter;
use Flow\ETL\IO\Formatter\Schema\PHPFormatter\ValueFormatter;
use Flow\ETL\IO\Formatter\Schema\PHPSchemaFormatter;
use Flow\ETL\IO\Formatter\Schema\SchemaFormatter;
use Flow\ETL\IO\Inference\SchemaInferenceBuilder;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\ArrayLoader;
use Flow\ETL\IO\Loader\MemoryLoader;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\IO\Loader\StreamLoader;
use Flow\ETL\IO\Loader\StreamLoader\Output;
use Flow\ETL\Kernels\Hash\Algorithm;
use Flow\ETL\Kernels\Hash\NativePHPHash;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\Constraint\SortedByConstraint;
use Flow\ETL\Operators\Constraint\UniqueConstraint;
use Flow\ETL\Operators\GroupBy\DeclaredPivotValues;
use Flow\ETL\Operators\GroupBy\DiscoveredPivotValues;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\Rename\RenameCaseColumnStrategy;
use Flow\ETL\Operators\Transformer\Rename\RenameMapColumnStrategy;
use Flow\ETL\Operators\Transformer\Rename\RenameReplaceColumnStrategy;
use Flow\ETL\Runtime\Analyze;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\SkipRows;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Runtime\Telemetry\TelemetryOptions;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\Memory\Memory;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\BatchSize as RowsBatchSize;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\DateDefinition;
use Flow\ETL\Values\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Values\Schema\Definition\EnumDefinition;
use Flow\ETL\Values\Schema\Definition\FloatDefinition;
use Flow\ETL\Values\Schema\Definition\HTMLDefinition;
use Flow\ETL\Values\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Flow\ETL\Values\Schema\Definition\JsonDefinition;
use Flow\ETL\Values\Schema\Definition\ListDefinition;
use Flow\ETL\Values\Schema\Definition\MapDefinition;
use Flow\ETL\Values\Schema\Definition\NullDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Definition\StructureDefinition;
use Flow\ETL\Values\Schema\Definition\TimeDefinition;
use Flow\ETL\Values\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Values\Schema\Definition\UuidDefinition;
use Flow\ETL\Values\Schema\Definition\XMLDefinition;
use Flow\ETL\Values\Schema\Definition\XMLElementDefinition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\ETL\Values\Schema\Metadata;
use Flow\ETL\Values\Schema\SortingStrategy;
use Flow\ETL\Values\Schema\SortingStrategy\AlphabeticalStrategy;
use Flow\ETL\Values\Schema\SortingStrategy\CombinedStrategy;
use Flow\ETL\Values\Schema\SortingStrategy\MetadataStrategy;
use Flow\ETL\Values\Schema\SortingStrategy\TypeStrategy;
use Flow\ETL\Values\Schema\SortingStrategy\TypeStrategy\TypePriorities as SchemaTypePriorities;
use Flow\ETL\Values\Schema\Validator\EvolvingValidator;
use Flow\ETL\Values\Schema\Validator\SelectiveValidator;
use Flow\ETL\Values\Schema\Validator\StrictValidator;
use Flow\ETL\Values\Schema\Validator\ValidationContext;
use Flow\ETL\Values\SchemaValidator;
use Flow\ETL\Values\SortOrder;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Mode;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Psr\Clock\ClockInterface;

use function array_is_list;
use function class_exists;
use function enum_exists;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_datetime;
use function is_array;
use function is_float;
use function is_int;
use function is_object;
use function is_string;
use function json_decode;
use function json_encode;
use function strtolower;

/**
 * Alias for data_frame() : Flow.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'writing', example: 'overwrite')]
function df(Config|ConfigBuilder|null $config = null): Flow
{
    return data_frame($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'writing', example: 'overwrite')]
function data_frame(Config|ConfigBuilder|null $config = null): Flow
{
    return new Flow($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function telemetry_options(
    bool $trace_loading = false,
    bool $trace_transformations = false,
    bool $trace_cache = false,
    bool $collect_metrics = false,
): TelemetryOptions {
    return new TelemetryOptions($trace_loading, $trace_transformations, $trace_cache, $collect_metrics);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'writing', example: 'overwrite')]
function from_rows(Rows ...$rows): RowsExtractor
{
    return new RowsExtractor(...$rows);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'partitioning', example: 'path_partitions')]
function from_path_partitions(
    Path|string $path,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): FileListingExtractor {
    return new FileListingExtractor(is_string($path) ? path($path) : $path, new PathPartitionsRow(), $filesystem);
}

/**
 * @param iterable<array<mixed>> $array
 * @param null|Schema $schema - @deprecated use withSchema() method instead
 * @param null|Path $spillRoot - where a non-array $array is spilled while it is described; null resolves to
 *                          $filesystem->getSystemTmpDir() and only on that path
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'reading', example: 'array')]
function from_array(
    iterable $array,
    ?Schema $schema = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
    ?Path $spillRoot = null,
): ArrayExtractor {
    $extractor = new ArrayExtractor($array, $filesystem, $spillRoot);

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
function from_cache(
    string $id,
    ?Extractor $fallback_extractor = null,
    bool $clear = false,
    ?RowsStorage $cache = null,
): CacheExtractor {
    $extractor = new CacheExtractor($id, $cache);

    if ($fallback_extractor !== null) {
        $extractor->withFallbackExtractor($fallback_extractor);
    }

    if ($clear) {
        $extractor->withClearOnFinish($clear);
    }

    return $extractor;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_all(Extractor ...$extractors): ChainExtractor
{
    return new ChainExtractor(...$extractors);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_memory(Memory $memory): ArrayExtractor
{
    return new ArrayExtractor($memory);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function files(string|Path $directory, Filesystem $filesystem = new NativeLocalFilesystem()): FileListingExtractor
{
    return new FileListingExtractor(
        is_string($directory) ? path($directory) : $directory,
        new FileMetadataRow(),
        $filesystem,
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function filesystem_cache(
    Path|string|null $cache_dir = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
    Backend $backend = new AdaptiveBackend(),
): FilesystemRowsStorage {
    return new FilesystemRowsStorage($filesystem, is_string($cache_dir) ? path_real($cache_dir) : $cache_dir, $backend);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period(
    string $column_name,
    DateTimeInterface $start,
    DateInterval $interval,
    DateTimeInterface $end,
    int $options = 0,
): SequenceExtractor {
    return new SequenceExtractor(
        new DatePeriodSequenceGenerator(new DatePeriod($start, $interval, $end, $options)),
        $column_name,
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period_recurrences(
    string $column_name,
    DateTimeInterface $start,
    DateInterval $interval,
    int $recurrences,
    int $options = 0,
): SequenceExtractor {
    return new SequenceExtractor(
        new DatePeriodSequenceGenerator(new DatePeriod($start, $interval, $recurrences, $options)),
        $column_name,
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_number(
    string $column_name,
    string|int|float $start,
    string|int|float $end,
    int|float $step = 1,
): SequenceExtractor {
    return new SequenceExtractor(new NumberSequenceGenerator($start, $end, $step), $column_name);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_memory(Memory $memory): MemoryLoader
{
    return new MemoryLoader($memory);
}

/**
 * Convert rows to an array and store them in passed array variable.
 *
 * @param array<array-key, mixed> $array
 *
 * @param-out array<array<mixed>> $array
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
#[DocumentationExample(topic: 'writing', example: 'array')]
function to_array(array &$array): ArrayLoader
{
    // @mago-ignore analysis:redundant-docblock-type
    /** @var array<array<mixed>> $array */
    return new ArrayLoader($array);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_output(
    int|bool $truncate = 20,
    Output $output = Output::rows,
    Formatter $formatter = new AsciiTableFormatter(),
    SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter(),
): StreamLoader {
    return StreamLoader::output($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stderr(
    int|bool $truncate = 20,
    Output $output = Output::rows,
    Formatter $formatter = new AsciiTableFormatter(),
    SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter(),
): StreamLoader {
    return StreamLoader::stderr($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stdout(
    int|bool $truncate = 20,
    Output $output = Output::rows,
    Formatter $formatter = new AsciiTableFormatter(),
    SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter(),
): StreamLoader {
    return StreamLoader::stdout($truncate, $output, $formatter, $schemaFormatter);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_stream(
    string $uri,
    int|bool $truncate = 20,
    Output $output = Output::rows,
    string $mode = 'w',
    Formatter $formatter = new AsciiTableFormatter(),
    SchemaFormatter $schemaFormatter = new ASCIISchemaFormatter(),
): StreamLoader {
    return new StreamLoader(
        $uri,
        Mode::from($mode),
        $truncate,
        $output,
        $formatter,
        $schemaFormatter,
        StreamLoader\Type::custom,
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_transformation(
    Transformer|Transformation $transformer,
    Loader|Branch|Transactional $write,
): TransformedBranch {
    return new TransformedBranch($transformer, $write);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_branch(ScalarFunction $condition, Loader|Branch|Transactional $write): FilteredBranch
{
    return new FilteredBranch($condition, $write);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_style(StringStyles $style): RenameCaseColumnStrategy
{
    return new RenameCaseColumnStrategy($style);
}

/**
 * @param array<string>|string $search
 * @param array<string>|string $replace
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_replace(string|array $search, string|array $replace): RenameReplaceColumnStrategy
{
    return new RenameReplaceColumnStrategy($search, $replace);
}

/**
 * @param array<array-key, string> $renames Map of old_name => new_name
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_map(array $renames): RenameMapColumnStrategy
{
    return new RenameMapColumnStrategy($renames);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function rows(Schema $schema, Backend $backend = new AdaptiveBackend()): Rows
{
    return Rows::empty($schema, $backend);
}

/**
 * An alias for `ref`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function col(string $column): UnresolvedReference
{
    return new UnresolvedReference($column);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
#[DocumentationExample(topic: 'columns', example: 'create')]
function ref(string $column): UnresolvedReference
{
    return new UnresolvedReference($column);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure_ref(string $column): StructureFunctions
{
    return ref($column)->structure();
}

/**
 * Builds a structure from scalar functions: one element per key, in key order.
 * An element is nullable when its function is; the structure itself never is.
 *
 * @param array<array-key, ScalarFunction> $elements
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure(array $elements): Structure
{
    return new Structure($elements);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function list_ref(string $column): ListFunctions
{
    return ref($column)->list();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function refs(string|Reference ...$columns): References
{
    return new References(...$columns);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function select(string|Reference ...$columns): Select
{
    return new Select(...$columns);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function drop(string|Reference ...$columns): Drop
{
    return new Drop(...$columns);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function add_row_index(string $column = 'index', StartFrom $startFrom = StartFrom::ZERO): AddRowIndex
{
    return new AddRowIndex($column, $startFrom);
}

/**
 * @param null|int<1, max> $min_size
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function batch_by(string|Reference $column, ?int $min_size = null): BatchBy
{
    return new BatchBy($column, $min_size);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function batch_size(int $size): BatchSize
{
    return new BatchSize(new RowsBatchSize($size));
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function limit(?int $limit): Limit
{
    return new Limit($limit);
}

/**
 * @param array<int, string> $columns
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function mask_columns(array $columns = [], string $mask = '******'): MaskColumns
{
    return new MaskColumns($columns, $mask);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function optional(ScalarFunction $function): Optional
{
    return new Optional($function);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
#[DocumentationExample(topic: 'columns', example: 'create')]
function lit(mixed $value): Literal
{
    return new Literal($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function exists(ScalarFunction $ref): Exists
{
    return new Exists($ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function when(mixed $condition, mixed $then, mixed $else = null): When
{
    return new When($condition, $then, $else);
}

/**
 * Alias for `array_get`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure_get(ScalarFunction $ref, string $path): ArrayGet
{
    return array_get($ref, $path);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get(ScalarFunction $ref, string $path): ArrayGet
{
    return new ArrayGet($ref, $path);
}

/**
 * Alias for `array_get_collection`.
 *
 * @param array<array-key, mixed>|ScalarFunction $keys
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure_get_collection(ScalarFunction $ref, ScalarFunction|array $keys): ArrayGetCollection
{
    return array_get_collection($ref, $keys);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $keys
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get_collection(ScalarFunction $ref, ScalarFunction|array $keys): ArrayGetCollection
{
    return new ArrayGetCollection($ref, $keys);
}

/**
 * Alias for `array_get_collection_first`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure_get_collection_first(ScalarFunction $ref, string ...$keys): ArrayGetCollection
{
    return array_get_collection_first($ref, ...$keys);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get_collection_first(ScalarFunction $ref, string ...$keys): ArrayGetCollection
{
    return ArrayGetCollection::fromFirst($ref, $keys);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $ref
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_exists(ScalarFunction|array $ref, ScalarFunction|string $path): ArrayPathExists
{
    return new ArrayPathExists($ref, $path);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $left
 * @param array<array-key, mixed>|ScalarFunction $right
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_merge(ScalarFunction|array $left, ScalarFunction|array $right): ArrayMerge
{
    return new ArrayMerge($left, $right);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $array
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_merge_collection(ScalarFunction|array $array): ArrayMergeCollection
{
    return new ArrayMergeCollection($array);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_key_rename(ScalarFunction $ref, string $path, string $newName): ArrayKeyRename
{
    return new ArrayKeyRename($ref, $path, $newName);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_keys_style_convert(
    ScalarFunction $ref,
    StringStyles|string $style = StringStyles::SNAKE,
): ArrayKeysStyleConvert {
    return new ArrayKeysStyleConvert($ref, $style instanceof StringStyles ? $style : StringStyles::fromString($style));
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_sort(
    ScalarFunction $function,
    ?Sort $sort_function = null,
    ScalarFunction|int|null $flags = null,
    ScalarFunction|bool $recursive = true,
): ArraySort {
    return new ArraySort($function, $sort_function ?? Sort::sort, $flags, $recursive);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $function
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_reverse(ScalarFunction|array $function, bool $preserveKeys = false): ArrayReverse
{
    return new ArrayReverse($function, $preserveKeys);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function now(DateTimeZone|ScalarFunction $time_zone = new DateTimeZone('UTC')): Now
{
    return new Now($time_zone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function between(
    mixed $value,
    mixed $lower_bound,
    mixed $upper_bound,
    ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE,
): Between {
    return new Between($value, $lower_bound, $upper_bound, $boundary);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_date_time(
    mixed $ref,
    ScalarFunction|string $format = 'Y-m-d H:i:s',
    ScalarFunction|DateTimeZone $timeZone = new DateTimeZone('UTC'),
): ToDateTime {
    return new ToDateTime($ref, $format, $timeZone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_date(
    mixed $ref,
    ScalarFunction|string $format = 'Y-m-d',
    ScalarFunction|DateTimeZone $timeZone = new DateTimeZone('UTC'),
): ToDate {
    return new ToDate($ref, $format, $timeZone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function date_time_format(ScalarFunction $ref, string $format): DateTimeFormat
{
    return new DateTimeFormat($ref, $format);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function split(
    ScalarFunction|string $value,
    ScalarFunction|string $separator,
    ScalarFunction|int $limit = PHP_INT_MAX,
): Split {
    return new Split($value, $separator, $limit);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $keys
 * @param array<array-key, mixed>|ScalarFunction $values
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function combine(ScalarFunction|array $keys, ScalarFunction|array $values): Combine
{
    return new Combine($keys, $values);
}

/**
 * Concat all values. If you want to concatenate values with separator use concat_ws function.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function concat(ScalarFunction|string ...$functions): Concat
{
    return new Concat(...$functions);
}

/**
 * Concat all values with separator.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function concat_ws(ScalarFunction|string $separator, ScalarFunction|string ...$functions): ConcatWithSeparator
{
    return new ConcatWithSeparator($separator, ...$functions);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function hash(mixed $value, Algorithm $algorithm = new NativePHPHash()): Hash
{
    return new Hash($value, $algorithm);
}

/**
 * @param \Flow\Types\Type<mixed>|string $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function cast(mixed $value, string|Type $type): Cast
{
    return new Cast($value, $type);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function coalesce(ScalarFunction ...$values): Coalesce
{
    return new Coalesce(...$values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function enum_name(mixed $value): EnumName
{
    return new EnumName($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function enum_value(mixed $value): EnumValue
{
    return new EnumValue($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function count(?Reference $function = null): Count
{
    return new Count($function);
}

/**
 * Calls a user-defined function with the given parameters.
 *
 * @param Type<mixed> $return_type
 * @param array<mixed> $parameters
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function call(ScalarFunction $callable, Type $return_type, array $parameters = []): CallUserFunc
{
    return new CallUserFunc($callable, $return_type, $parameters);
}

/**
 * Unpacks each element of an array into a new column, using the array key as the column name.
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
 *
 * @param array<array-key, mixed>|ScalarFunction $array
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_unpack(ScalarFunction|array $array, Schema $schema): ArrayUnpack
{
    return new ArrayUnpack($array, $schema);
}

/**
 * Expands each value into a column, if there are more than one value, multiple rows will be created.
 * Array keys are ignored, only values are used to create new rows.
 * Nested in another function (structure(), concat(), ...) it still gives one row per element. Several
 * expands in one expression are zipped to the longest list; a shorter one gives null, so its element
 * type becomes nullable. It is refused inside another array_expand() and in filter(), until(),
 * duplicateRow(), aggregate(), over() and onEach().
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
function array_expand(ScalarFunction $function, ArrayExpand $expand = ArrayExpand::VALUES): ArrayExpandFunction
{
    return new ArrayExpandFunction($function, $expand);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function size(mixed $value): Size
{
    return new Size($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function uuid_v4(): Uuid
{
    return Uuid::uuid4();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function uuid_v7(ScalarFunction|DateTimeInterface $value): Uuid
{
    return Uuid::uuid7($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function ulid(ScalarFunction|string|null $value = null): Ulid
{
    return new Ulid($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function lower(ScalarFunction|string $value): ToLower
{
    return new ToLower($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function capitalize(ScalarFunction|string $value): Capitalize
{
    return new Capitalize($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function upper(ScalarFunction|string $value): ToUpper
{
    return new ToUpper($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function all(ScalarFunction ...$functions): All
{
    return new All(...$functions);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function any(ScalarFunction ...$values): Any
{
    return new Any(...$values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function not(ScalarFunction $value): Not
{
    return new Not($value);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function to_timezone(ScalarFunction|DateTimeInterface $value, DateTimeZone|string $timeZone): ToTimeZone
{
    return new ToTimeZone($value, $timeZone);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function ignore_error_handler(): IgnoreError
{
    return new IgnoreError();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function skip_rows_handler(): SkipRows
{
    return new SkipRows();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function throw_error_handler(): ThrowError
{
    return new ThrowError();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_replace(
    ScalarFunction|string $pattern,
    ScalarFunction|string $replacement,
    ScalarFunction|string $subject,
    ScalarFunction|int|null $limit = null,
): RegexReplace {
    return new RegexReplace($pattern, $replacement, $subject, $limit);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_match_all(
    ScalarFunction|string $pattern,
    ScalarFunction|string $subject,
    ScalarFunction|int $flags = 0,
    ScalarFunction|int $offset = 0,
): RegexMatchAll {
    return new RegexMatchAll($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_match(
    ScalarFunction|string $pattern,
    ScalarFunction|string $subject,
    ScalarFunction|int $flags = 0,
    ScalarFunction|int $offset = 0,
): RegexMatch {
    return new RegexMatch($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex(
    ScalarFunction|string $pattern,
    ScalarFunction|string $subject,
    int $flags = 0,
    ScalarFunction|int $offset = 0,
): Regex {
    return new Regex($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_all(
    ScalarFunction|string $pattern,
    ScalarFunction|string $subject,
    int $flags = 0,
    ScalarFunction|int $offset = 0,
): RegexAll {
    return new RegexAll($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function sprintf(ScalarFunction|string $format, ScalarFunction|float|int|string|null ...$args): Sprintf
{
    return new Sprintf($format, ...$args);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function sanitize(
    ScalarFunction|string $value,
    ScalarFunction|string $placeholder = '*',
    ScalarFunction|int|null $skipCharacters = null,
): Sanitize {
    return new Sanitize($value, $placeholder, $skipCharacters);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function round(
    ScalarFunction|int|float $value,
    ScalarFunction|int $precision = 2,
    ScalarFunction|int $mode = PHP_ROUND_HALF_UP,
): Round {
    return new Round($value, $precision, $mode);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function number_format(
    ScalarFunction|int|float $value,
    ScalarFunction|int $decimals = 2,
    ScalarFunction|string $decimal_separator = '.',
    ScalarFunction|string $thousands_separator = ',',
): NumberFormat {
    return new NumberFormat($value, $decimals, $decimal_separator, $thousands_separator);
}

/**
 * @param array<array<mixed>>|array<mixed|string> $data
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function array_to_rows(array $data, Schema $schema, Backend $backend = new AdaptiveBackend()): Rows
{
    return (new RowsBuilder($schema, $backend))
        ->appendRecords($data)
        ->finish();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function rank(): Rank
{
    return new Rank();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function dens_rank(): DenseRank
{
    return dense_rank();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::WINDOW_FUNCTION)]
function dense_rank(): DenseRank
{
    return new DenseRank();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function average(
    Reference|string $ref,
    int $scale = 2,
    Rounding $rounding = Rounding::HALF_UP,
    bool $exact = false,
): Average {
    return new Average(is_string($ref) ? ref($ref) : $ref, $scale, $rounding, $exact);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function greatest(mixed ...$values): Greatest
{
    return new Greatest($values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function least(mixed ...$values): Least
{
    return new Least($values);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect(Reference|string $ref): Collect
{
    return new Collect(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function string_agg(Reference|string $ref, string $separator = ', ', ?SortOrder $sort = null): StringAggregate
{
    return new StringAggregate(is_string($ref) ? ref($ref) : $ref, $separator, $sort);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect_unique(Reference|string $ref): CollectUnique
{
    return new CollectUnique(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function window(): Window
{
    return new Window();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function unbounded_preceding(): FrameBound
{
    return new FrameBound(FrameBoundType::UNBOUNDED_PRECEDING);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function preceding(int $offset): FrameBound
{
    return new FrameBound(FrameBoundType::PRECEDING, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function current_row(): FrameBound
{
    return new FrameBound(FrameBoundType::CURRENT_ROW);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function following(int $offset): FrameBound
{
    return new FrameBound(FrameBoundType::FOLLOWING, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function unbounded_following(): FrameBound
{
    return new FrameBound(FrameBoundType::UNBOUNDED_FOLLOWING);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function sum(Reference|string $ref, bool $exact = false): Sum
{
    return new Sum(is_string($ref) ? ref($ref) : $ref, $exact);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function first(Reference|string $ref): First
{
    return new First(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function last(Reference|string $ref): Last
{
    return new Last(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function max(Reference|string $ref): Max
{
    return new Max(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function min(Reference|string $ref): Min
{
    return new Min(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function row_number(): RowNumber
{
    return new RowNumber();
}

/**
 * @param Definition<mixed> ...$definitions
 *
 * @return Schema
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema(Definition ...$definitions): Schema
{
    return new Schema(...$definitions);
}

/**
 * @param Schema $schema
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_to_json(Schema $schema, bool $pretty = false): string
{
    return (new JsonSchemaFormatter($pretty))->format($schema);
}

/**
 * @param Schema $schema
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_to_php(
    Schema $schema,
    ValueFormatter $valueFormatter = new ValueFormatter(),
    TypeFormatter $typeFormatter = new TypeFormatter(),
): string {
    return (new PHPSchemaFormatter($valueFormatter, $typeFormatter))->format($schema);
}

/**
 * @param Schema $schema
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_to_ascii(Schema $schema, ?SchemaFormatter $formatter = null): string
{
    return ($formatter ?? new ASCIISchemaFormatter())->format($schema);
}

/**
 * @param Schema $expected
 * @param Schema $given
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_validate(
    Schema $expected,
    Schema $given,
    SchemaValidator $validator = new StrictValidator(),
): ValidationContext {
    return $validator->validate($expected, $given);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_evolving_validator(): EvolvingValidator
{
    return new EvolvingValidator();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_strict_validator(): StrictValidator
{
    return new StrictValidator();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_selective_validator(): SelectiveValidator
{
    return new SelectiveValidator();
}

/**
 * @return Schema
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_from_json(string $schema): Schema
{
    // @mago-ignore analysis:mixed-assignment
    $decodedSchema = json_decode($schema, true, 512, JSON_THROW_ON_ERROR);
    $decodedSchema = type_array()->assert($decodedSchema);

    return Schema::fromArray($decodedSchema);
}

/**
 * @param array<string, array<bool|float|int|string>|bool|float|int|string> $metadata
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function schema_metadata(array $metadata = []): Metadata
{
    return Metadata::fromArray($metadata);
}

/**
 * Alias for `integer_schema`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function int_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): IntegerDefinition
{
    return integer_schema($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function integer_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): IntegerDefinition
{
    return new IntegerDefinition($name, $nullable, $metadata);
}

/**
 * Alias for `string_schema`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function str_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): StringDefinition
{
    return string_schema($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function string_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): StringDefinition
{
    return new StringDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function bool_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): BooleanDefinition
{
    return new BooleanDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function float_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): FloatDefinition
{
    return new FloatDefinition($name, $nullable, $metadata);
}

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @param MapType<TKey, TValue>|Type<array<TKey, TValue>> $type
 *
 * @return MapDefinition<TKey, TValue>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function map_schema(string $name, MapType|Type $type, bool $nullable = false, ?Metadata $metadata = null): MapDefinition
{
    /** @var MapType<TKey, TValue> $type */
    return new MapDefinition($name, $type, $nullable, $metadata);
}

/**
 * @template T
 *
 * @param ListType<T>|Type<list<T>> $type
 *
 * @return ListDefinition<T>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function list_schema(
    string $name,
    ListType|Type $type,
    bool $nullable = false,
    ?Metadata $metadata = null,
): ListDefinition {
    /** @var ListType<T> $type */
    return new ListDefinition($name, $type, $nullable, $metadata);
}

/**
 * @template T of \UnitEnum
 *
 * @param class-string<T> $type
 *
 * @return EnumDefinition<T>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function enum_schema(string $name, string $type, bool $nullable = false, ?Metadata $metadata = null): EnumDefinition
{
    return new EnumDefinition($name, $type, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function null_schema(string $name, ?Metadata $metadata = null): NullDefinition
{
    return new NullDefinition($name, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function datetime_schema(
    string $name,
    bool $nullable = false,
    ?Metadata $metadata = null,
    DateTimeZone|string $zone = 'UTC',
): DateTimeDefinition {
    return new DateTimeDefinition($name, type_datetime($zone), $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function time_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): TimeDefinition
{
    return new TimeDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function date_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): DateDefinition
{
    return new DateDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function json_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): JsonDefinition
{
    return new JsonDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function html_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): HTMLDefinition
{
    return new HTMLDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function html_element_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): HTMLElementDefinition
{
    return new HTMLElementDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): XMLDefinition
{
    return new XMLDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function xml_element_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): XMLElementDefinition
{
    return new XMLElementDefinition($name, $nullable, $metadata);
}

/**
 * @template T
 *
 * @param StructureType<array<array-key, T>>|Type<array<array-key, T>> $type
 *
 * @return StructureDefinition<T>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function structure_schema(
    string $name,
    StructureType|Type $type,
    bool $nullable = false,
    ?Metadata $metadata = null,
): StructureDefinition {
    /** @var StructureType<array<array-key, T>> $type */
    return new StructureDefinition($name, $type, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function uuid_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): UuidDefinition
{
    return new UuidDefinition($name, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function time_zone_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): TimeZoneDefinition
{
    return new TimeZoneDefinition($name, $nullable, $metadata);
}

/**
 * Create a Definition from an array representation.
 *
 * @param array<array-key, mixed> $definition
 *
 * @return Definition<mixed>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function definition_from_array(array $definition): Definition
{
    return Definitions::fromArray($definition);
}

/**
 * Create a Definition from a Type.
 *
 * @param Type<mixed> $type
 *
 * @return Definition<mixed>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function definition_from_type(
    string|Reference $ref,
    Type $type,
    bool $nullable = false,
    ?Metadata $metadata = null,
): Definition {
    return Definitions::of(is_string($ref) ? $ref : $ref->name(), $type, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function infer_schema(): SchemaInferenceBuilder
{
    return new SchemaInferenceBuilder();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function execution_context(?Config $config = null): FlowContext
{
    return new FlowContext($config ?? (new ConfigBuilder())->build());
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function flow_context(?Config $config = null): FlowContext
{
    return execution_context($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config(): Config
{
    return (new ConfigBuilder())->build();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config_builder(): ConfigBuilder
{
    return new ConfigBuilder();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function memory_sort(): MemorySortBuilder
{
    return new MemorySortBuilder();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function external_sort(): ExternalSortBuilder
{
    return new ExternalSortBuilder();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function hash_join(): HashJoinBuilder
{
    return new HashJoinBuilder();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function hash_group_by(): BucketingConfigBuilder
{
    return BucketingConfigBuilder::groupBy();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function hash_repartition(): BucketingConfigBuilder
{
    return BucketingConfigBuilder::repartition();
}

/**
 * Declares the pivot columns a groupBy()->pivot() produces, so the plan can name them before a row flows.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function pivot_values(int|string ...$values): DeclaredPivotValues
{
    return new DeclaredPivotValues(...$values);
}

/**
 * Reads the pivot column once at build time and turns what it finds into declared values. Refuses a
 * source that cannot be read twice.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function discover_pivot_values(int $maxValues = 10_000): DiscoveredPivotValues
{
    return new DiscoveredPivotValues($maxValues);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function partition_by(string|Reference $column, string|Reference ...$columns): Partitioning
{
    return Partitioning::by($column, ...$columns);
}

/**
 * @param Type<mixed> ...$types partition column name => type, passed as named arguments
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function partition_types(Type ...$types): PartitionTypes
{
    /** @var array<string, Type<mixed>> $types */
    return new PartitionTypes($types);
}

/**
 * Alias for save_mode_overwrite().
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function overwrite(): SaveMode
{
    return SaveMode::Overwrite;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function save_mode_overwrite(): SaveMode
{
    return SaveMode::Overwrite;
}

/**
 * Alias for save_mode_ignore().
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function ignore(): SaveMode
{
    return SaveMode::Ignore;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function save_mode_ignore(): SaveMode
{
    return SaveMode::Ignore;
}

/**
 * Alias for save_mode_exception_if_exists().
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function exception_if_exists(): SaveMode
{
    return SaveMode::ExceptionIfExists;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function save_mode_exception_if_exists(): SaveMode
{
    return SaveMode::ExceptionIfExists;
}

/**
 * Alias for save_mode_append().
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function append(): SaveMode
{
    return SaveMode::Append;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function save_mode_append(): SaveMode
{
    return SaveMode::Append;
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function print_rows(Rows $rows, int|bool $truncate = false, ?Formatter $formatter = null): string
{
    return ($formatter ?? new AsciiTableFormatter())->format($rows, $truncate);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function identical(Reference|string $left, Reference|string $right): Identical
{
    return new Identical($left, $right);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function equal(Reference|string $left, Reference|string $right): Equal
{
    return new Equal($left, $right);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function compare_all(Condition $comparison, Condition ...$comparisons): Condition\All
{
    return new Condition\All($comparison, ...$comparisons);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function compare_any(Condition $comparison, Condition ...$comparisons): Condition\Any
{
    return new Condition\Any($comparison, ...$comparisons);
}

/**
 * @param array<Condition|string>|Condition $comparisons
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'joins', example: 'join')]
#[DocumentationExample(topic: 'joins', example: 'join_each')]
function join_on(array|Condition $comparisons, string $join_prefix = ''): Expression
{
    return Expression::on($comparisons, $join_prefix);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_sort_by_name(SortOrder $order = SortOrder::ASC): SortingStrategy
{
    return new AlphabeticalStrategy($order);
}

/**
 * @param array<class-string<Type<mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_sort_by_type(
    array $priorities = SchemaTypePriorities::PRIORITIES,
    SortOrder $order = SortOrder::ASC,
): SortingStrategy {
    return new TypeStrategy(new SchemaTypePriorities($priorities), $order);
}

/**
 * @param array<class-string<Type<mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_sort_by_type_and_name(
    array $priorities = SchemaTypePriorities::PRIORITIES,
    SortOrder $order = SortOrder::ASC,
): SortingStrategy {
    return new CombinedStrategy(
        new TypeStrategy(new SchemaTypePriorities($priorities), $order),
        new AlphabeticalStrategy($order),
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function schema_sort_by_metadata(string $key, SortOrder $order = SortOrder::ASC): SortingStrategy
{
    return new MetadataStrategy($key, $order);
}

/**
 * @param array<string|Type<mixed>>|Type<mixed> $type
 * @param mixed $value
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function is_type(Type|array $type, mixed $value): bool
{
    if ($type instanceof Type) {
        $type = [$type];
    }

    foreach ($type as $nextType) {
        if (is_string($nextType)) {
            if (match (strtolower($nextType)) {
                'str', 'string' => is_string($value),
                'int', 'integer' => is_int($value),
                'float' => is_float($value),
                'null' => null === $value,
                'object' => is_object($value),
                'array' => is_array($value),
                'list' => is_array($value) && array_is_list($value),
                default => match (class_exists($nextType) || enum_exists($nextType)) {
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

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function generate_random_string(
    int $length = 32,
    RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
): string {
    return $generator->string($length);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function generate_random_int(
    int $start = PHP_INT_MIN,
    int $end = PHP_INT_MAX,
    RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
): int {
    return $generator->int($start, $end);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function random_string(
    int|ScalarFunction $length,
    RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
): RandomString {
    return new RandomString($length, $generator);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function date_interval_to_milliseconds(DateInterval $interval): int
{
    if ($interval->y !== 0 || $interval->m !== 0) {
        throw new InvalidArgumentException(
            "Relative DateInterval (with months/years) can't be converted to milliseconds. Given"
                . json_encode($interval, JSON_THROW_ON_ERROR),
        );
    }

    $absoluteSeconds = ($interval->d * 24 * 60 * 60) + ($interval->h * 60 * 60) + ($interval->i * 60) + $interval->s;

    return $interval->invert
        ? -(int) (($absoluteSeconds * 1000) + ($interval->f * 1000))
        : (int) (($absoluteSeconds * 1000) + ($interval->f * 1000));
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function date_interval_to_seconds(DateInterval $interval): int
{
    if ($interval->y !== 0 || $interval->m !== 0) {
        throw new InvalidArgumentException(
            "Relative DateInterval (with months/years) can't be converted to seconds. Given"
                . json_encode($interval, JSON_THROW_ON_ERROR),
        );
    }

    $absoluteSeconds = ($interval->d * 24 * 60 * 60) + ($interval->h * 60 * 60) + ($interval->i * 60) + $interval->s;

    return $interval->invert
        ? -(int) ceil($absoluteSeconds + $interval->f)
        : (int) ceil($absoluteSeconds + $interval->f);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function date_interval_to_microseconds(DateInterval $interval): int
{
    if ($interval->y !== 0 || $interval->m !== 0) {
        throw new InvalidArgumentException(
            "Relative DateInterval (with months/years) can't be converted to microseconds. Given"
                . json_encode($interval, JSON_THROW_ON_ERROR),
        );
    }

    $absoluteSeconds = ($interval->d * 24 * 60 * 60) + ($interval->h * 60 * 60) + ($interval->i * 60) + $interval->s;

    return $interval->invert
        ? -(int) (($absoluteSeconds * 1000000) + ($interval->f * 1000000))
        : (int) (($absoluteSeconds * 1000000) + ($interval->f * 1000000));
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function with_column(string $name, ScalarFunction $function): WithColumn
{
    return new WithColumn($name, $function);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function constraint_unique(string $reference, string ...$references): UniqueConstraint
{
    return new UniqueConstraint($reference, ...$references);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function constraint_sorted_by(
    string|Reference|SortColumn $column,
    string|Reference|SortColumn ...$columns,
): SortedByConstraint {
    return new SortedByConstraint($column, ...$columns);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function analyze(): Analyze
{
    return new Analyze();
}

/**
 * @param array<MatchCondition> $cases
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function match_cases(array $cases, mixed $default = null): MatchCases
{
    return new MatchCases($cases, $default);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function match_condition(mixed $condition, mixed $then): MatchCondition
{
    return new MatchCondition($condition, $then);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function clock(string $time_zone = 'UTC'): ClockInterface
{
    return new SystemClock(new DateTimeZone($time_zone));
}
