<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Brick\Math\BigDecimal;
use DateInterval;
use DatePeriod;
use DateTime;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Dom\Element;
use Dom\HTMLDocument;
use Dom\HTMLElement;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Exception;
use Flow\Calculator\Rounding;
use Flow\Clock\SystemClock;
use Flow\ETL\Analyze;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\DocumentationExample;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Config;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\ETL\Constraint\SortedByConstraint;
use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\DataFrame;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\SkipRows;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\ArrayExtractor;
use Flow\ETL\Extractor\BatchByExtractor;
use Flow\ETL\Extractor\BatchExtractor;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Extractor\ChainExtractor;
use Flow\ETL\Extractor\DataFrameExtractor;
use Flow\ETL\Extractor\FilesExtractor;
use Flow\ETL\Extractor\MemoryExtractor;
use Flow\ETL\Extractor\PathPartitionsExtractor;
use Flow\ETL\Extractor\PipelineExtractor;
use Flow\ETL\Extractor\RowsExtractor;
use Flow\ETL\Extractor\SequenceExtractor;
use Flow\ETL\Extractor\SequenceGenerator\DatePeriodSequenceGenerator;
use Flow\ETL\Extractor\SequenceGenerator\NumberSequenceGenerator;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Formatter;
use Flow\ETL\Formatter\AsciiTableFormatter;
use Flow\ETL\Function\All;
use Flow\ETL\Function\Any;
use Flow\ETL\Function\ArrayExpand as ArrayExpandFunction;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArrayGet;
use Flow\ETL\Function\ArrayGetCollection;
use Flow\ETL\Function\ArrayKeyRename;
use Flow\ETL\Function\ArrayKeysStyleConvert;
use Flow\ETL\Function\ArrayMerge;
use Flow\ETL\Function\ArrayMergeCollection;
use Flow\ETL\Function\ArrayPathExists;
use Flow\ETL\Function\ArrayReverse;
use Flow\ETL\Function\ArraySort;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\ArrayUnpack;
use Flow\ETL\Function\Average;
use Flow\ETL\Function\Between;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\CallUserFunc;
use Flow\ETL\Function\Capitalize;
use Flow\ETL\Function\Cast;
use Flow\ETL\Function\Coalesce;
use Flow\ETL\Function\Collect;
use Flow\ETL\Function\CollectUnique;
use Flow\ETL\Function\Combine;
use Flow\ETL\Function\Concat;
use Flow\ETL\Function\ConcatWithSeparator;
use Flow\ETL\Function\Count;
use Flow\ETL\Function\DateTimeFormat;
use Flow\ETL\Function\DenseRank;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Function\Exists;
use Flow\ETL\Function\First;
use Flow\ETL\Function\Greatest;
use Flow\ETL\Function\Hash;
use Flow\ETL\Function\Last;
use Flow\ETL\Function\Least;
use Flow\ETL\Function\ListFunctions;
use Flow\ETL\Function\Literal;
use Flow\ETL\Function\MatchCases;
use Flow\ETL\Function\MatchCases\MatchCondition;
use Flow\ETL\Function\Max;
use Flow\ETL\Function\Min;
use Flow\ETL\Function\Not;
use Flow\ETL\Function\Now;
use Flow\ETL\Function\NumberFormat;
use Flow\ETL\Function\Optional;
use Flow\ETL\Function\RandomString;
use Flow\ETL\Function\Rank;
use Flow\ETL\Function\Regex;
use Flow\ETL\Function\RegexAll;
use Flow\ETL\Function\RegexMatch;
use Flow\ETL\Function\RegexMatchAll;
use Flow\ETL\Function\RegexReplace;
use Flow\ETL\Function\Round;
use Flow\ETL\Function\RowNumber;
use Flow\ETL\Function\Sanitize;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\Size;
use Flow\ETL\Function\Split;
use Flow\ETL\Function\Sprintf;
use Flow\ETL\Function\StringAggregate;
use Flow\ETL\Function\StructureFunctions;
use Flow\ETL\Function\Sum;
use Flow\ETL\Function\ToDate;
use Flow\ETL\Function\ToDateTime;
use Flow\ETL\Function\ToLower;
use Flow\ETL\Function\ToTimeZone;
use Flow\ETL\Function\ToUpper;
use Flow\ETL\Function\Ulid;
use Flow\ETL\Function\Uuid;
use Flow\ETL\Function\When;
use Flow\ETL\Hash\Algorithm;
use Flow\ETL\Hash\NativePHPHash;
use Flow\ETL\Join\Comparison;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Join\Expression;
use Flow\ETL\Loader;
use Flow\ETL\Loader\ArrayLoader;
use Flow\ETL\Loader\BranchingLoader;
use Flow\ETL\Loader\CallbackLoader;
use Flow\ETL\Loader\MemoryLoader;
use Flow\ETL\Loader\RetryLoader;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Loader\TransformerLoader;
use Flow\ETL\Memory\Memory;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Pipeline;
use Flow\ETL\RandomValueGenerator;
use Flow\ETL\Retry\DelayFactory;
use Flow\ETL\Retry\DelayFactory\Exponential;
use Flow\ETL\Retry\DelayFactory\Fixed;
use Flow\ETL\Retry\DelayFactory\Fixed\FixedMilliseconds;
use Flow\ETL\Retry\DelayFactory\Jitter;
use Flow\ETL\Retry\DelayFactory\Linear;
use Flow\ETL\Retry\RetryStrategy;
use Flow\ETL\Retry\RetryStrategy\AnyThrowable;
use Flow\ETL\Retry\RetryStrategy\OnExceptionTypes;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\HTMLElementEntry;
use Flow\ETL\Row\Entry\HTMLEntry;
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
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\DateDefinition;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Definition\HTMLDefinition;
use Flow\ETL\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Definition\MapDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\StructureDefinition;
use Flow\ETL\Schema\Definition\TimeDefinition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Definition\UuidDefinition;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Definition\XMLElementDefinition;
use Flow\ETL\Schema\Formatter\JsonSchemaFormatter;
use Flow\ETL\Schema\Formatter\PHPFormatter\TypeFormatter;
use Flow\ETL\Schema\Formatter\PHPFormatter\ValueFormatter;
use Flow\ETL\Schema\Formatter\PHPSchemaFormatter;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Schema\SchemaFormatter;
use Flow\ETL\Schema\SortingStrategy;
use Flow\ETL\Schema\SortingStrategy\AlphabeticalStrategy;
use Flow\ETL\Schema\SortingStrategy\CombinedStrategy;
use Flow\ETL\Schema\SortingStrategy\MetadataStrategy;
use Flow\ETL\Schema\SortingStrategy\TypeStrategy;
use Flow\ETL\Schema\SortingStrategy\TypeStrategy\TypePriorities as SchemaTypePriorities;
use Flow\ETL\Schema\Validator\EvolvingValidator;
use Flow\ETL\Schema\Validator\SelectiveValidator;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Schema\Validator\ValidationContext;
use Flow\ETL\SchemaValidator;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Time\Duration;
use Flow\ETL\Time\Sleep;
use Flow\ETL\Time\SystemSleep;
use Flow\ETL\Transformation;
use Flow\ETL\Transformation\AddRowIndex;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformation\BatchSize;
use Flow\ETL\Transformation\Drop;
use Flow\ETL\Transformation\Limit;
use Flow\ETL\Transformation\MaskColumns;
use Flow\ETL\Transformation\Select;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\OrderEntries\CombinedComparator;
use Flow\ETL\Transformer\OrderEntries\Comparator;
use Flow\ETL\Transformer\OrderEntries\NameComparator;
use Flow\ETL\Transformer\OrderEntries\Order;
use Flow\ETL\Transformer\OrderEntries\TypeComparator;
use Flow\ETL\Transformer\OrderEntries\TypePriorities;
use Flow\ETL\Transformer\Rename\RenameCaseEntryStrategy;
use Flow\ETL\Transformer\Rename\RenameMapEntryStrategy;
use Flow\ETL\Transformer\Rename\RenameReplaceEntryStrategy;
use Flow\ETL\Window;
use Flow\ETL\WithEntry;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Mode;
use Flow\Filesystem\Telemetry\FilesystemTelemetryOptions;
use Flow\Serializer\NativePHPSerializer;
use Flow\Serializer\Serializer;
use Flow\Types\Type;
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
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Type\TypeFactory;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid as FlowUuid;
use Psr\Clock\ClockInterface;
use ReflectionProperty;
use Throwable;
use UnitEnum;

use function array_is_list;
use function array_key_exists;
use function array_map;
use function array_values;
use function class_exists;
use function enum_exists;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_array;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_string;
use function json_decode;
use function json_encode;
use function str_pad;
use function strtolower;

/**
 * Alias for data_frame() : Flow.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'data_frame', example: 'data_reading', option: 'data_frame')]
#[DocumentationExample(topic: 'data_frame', example: 'data_writing', option: 'overwrite')]
function df(Config|ConfigBuilder|null $config = null): Flow
{
    return data_frame($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'data_frame', example: 'data_reading', option: 'data_frame')]
#[DocumentationExample(topic: 'data_frame', example: 'data_writing', option: 'overwrite')]
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
    ?FilesystemTelemetryOptions $filesystem = null,
): TelemetryOptions {
    return new TelemetryOptions(
        $trace_loading,
        $trace_transformations,
        $trace_cache,
        $collect_metrics,
        $filesystem ?? new FilesystemTelemetryOptions(),
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_frame', example: 'data_reading', option: 'data_frame')]
#[DocumentationExample(topic: 'data_frame', example: 'data_writing', option: 'overwrite')]
function from_rows(Rows ...$rows): RowsExtractor
{
    return new RowsExtractor(...$rows);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'partitioning', example: 'path_partitions')]
function from_path_partitions(Path|string $path): PathPartitionsExtractor
{
    return new PathPartitionsExtractor(is_string($path) ? path($path) : $path);
}

/**
 * @param iterable<array<mixed>> $array
 * @param null|Schema $schema - @deprecated use withSchema() method instead
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_frame', example: 'data_reading', option: 'array')]
#[DocumentationExample(topic: 'data_frame', example: 'data_reading', option: 'data_frame')]
function from_array(iterable $array, ?Schema $schema = null): ArrayExtractor
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
function from_cache(string $id, ?Extractor $fallback_extractor = null, bool $clear = false): CacheExtractor
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
function from_all(Extractor ...$extractors): ChainExtractor
{
    return new ChainExtractor(...$extractors);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_memory(Memory $memory): MemoryExtractor
{
    return new MemoryExtractor($memory);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function files(string|Path $directory): FilesExtractor
{
    return new FilesExtractor(is_string($directory) ? path($directory) : $directory);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function filesystem_cache(
    Path|string|null $cache_dir = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
    Serializer $serializer = new NativePHPSerializer(),
): FilesystemCache {
    return new FilesystemCache($filesystem, $serializer, is_string($cache_dir) ? path_real($cache_dir) : $cache_dir);
}

/**
 * @param null|int<1, max> $min_size
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function batched_by(Extractor $extractor, string|Reference $column, ?int $min_size = null): BatchByExtractor
{
    // @mago-ignore analysis:invalid-operand
    if ($min_size !== null && $min_size <= 0) {
        throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $min_size);
    }

    return new BatchByExtractor($extractor, EntryReference::init($column), $min_size);
}

/**
 * @param int<1, max> $size
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function batches(Extractor $extractor, int $size): BatchExtractor
{
    return new BatchExtractor($extractor, $size);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_pipeline(Pipeline $pipeline): PipelineExtractor
{
    return new PipelineExtractor($pipeline);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_data_frame(DataFrame $data_frame): DataFrameExtractor
{
    return new DataFrameExtractor($data_frame);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period(
    string $entry_name,
    DateTimeInterface $start,
    DateInterval $interval,
    DateTimeInterface $end,
    int $options = 0,
): SequenceExtractor {
    return new SequenceExtractor(
        new DatePeriodSequenceGenerator(new DatePeriod($start, $interval, $end, $options)),
        $entry_name,
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_date_period_recurrences(
    string $entry_name,
    DateTimeInterface $start,
    DateInterval $interval,
    int $recurrences,
    int $options = 0,
): SequenceExtractor {
    return new SequenceExtractor(
        new DatePeriodSequenceGenerator(new DatePeriod($start, $interval, $recurrences, $options)),
        $entry_name,
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::EXTRACTOR)]
function from_sequence_number(
    string $entry_name,
    string|int|float $start,
    string|int|float $end,
    int|float $step = 1,
): SequenceExtractor {
    return new SequenceExtractor(new NumberSequenceGenerator($start, $end, $step), $entry_name);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_callable(callable $callable): CallbackLoader
{
    return new CallbackLoader($callable);
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
#[DocumentationExample(topic: 'data_frame', example: 'data_writing', option: 'array')]
function to_array(array &$array): ArrayLoader
{
    // @mago-ignore analysis:redundant-docblock-type
    /** @phpstan-var array<array<mixed>> $array */
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
function to_transformation(Transformer|Transformation $transformer, Loader $loader): TransformerLoader
{
    return new TransformerLoader($transformer, $loader);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function to_branch(ScalarFunction $condition, Loader $loader): BranchingLoader
{
    return new BranchingLoader($condition, $loader);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_style(StringStyles $style): RenameCaseEntryStrategy
{
    return new RenameCaseEntryStrategy($style);
}

/**
 * @param array<string>|string $search
 * @param array<string>|string $replace
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_replace(string|array $search, string|array $replace): RenameReplaceEntryStrategy
{
    return new RenameReplaceEntryStrategy($search, $replace);
}

/**
 * @param array<string, string> $renames Map of old_name => new_name
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function rename_map(array $renames): RenameMapEntryStrategy
{
    return new RenameMapEntryStrategy($renames);
}

/**
 * @return ($value is null ? Entry<null> : Entry<bool>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function bool_entry(string $name, ?bool $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new BooleanEntry($name, null, $metadata);
    }

    return new BooleanEntry($name, $value, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<bool>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function boolean_entry(string $name, ?bool $value, ?Metadata $metadata = null): Entry
{
    return bool_entry($name, $value, $metadata);
}

/**
 * @throws InvalidArgumentException
 *
 * @return ($value is null ? Entry<null> : Entry<\DateTimeInterface>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function datetime_entry(string $name, DateTimeInterface|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new DateTimeEntry($name, null, $metadata);
    }

    if ($value instanceof DateTime) {
        return new DateTimeEntry($name, DateTimeImmutable::createFromMutable($value), $metadata);
    }

    if ($value instanceof DateTimeInterface) {
        return new DateTimeEntry($name, $value, $metadata);
    }

    try {
        return new DateTimeEntry($name, new DateTimeImmutable($value), $metadata);
    } catch (Exception $e) {
        throw new InvalidArgumentException(
            "Invalid value given: '{$value}', reason: " . $e->getMessage(),
            previous: $e,
        );
    }
}

/**
 * @throws InvalidArgumentException
 *
 * @return ($value is null ? Entry<null> : Entry<\DateInterval>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function time_entry(string $name, DateInterval|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new TimeEntry($name, null, $metadata);
    }

    if ($value instanceof DateInterval) {
        return new TimeEntry($name, $value, $metadata);
    }

    try {
        return new TimeEntry($name, new DateInterval($value), $metadata);
    } catch (Throwable $dateIntervalException) {
        try {
            $dateTime = new DateTimeImmutable($value);
            $hours = (int) $dateTime->format('H');
            $minutes = (int) $dateTime->format('i');
            $seconds = (int) $dateTime->format('s');
            $fraction = (int) $dateTime->format('u') / 1_000_000;

            $interval = new DateInterval('PT' . $hours . 'H' . $minutes . 'M' . $seconds . 'S');
            (new ReflectionProperty($interval, 'f'))->setValue($interval, $fraction);

            return new TimeEntry($name, $interval, $metadata);
        } catch (Throwable) {
            throw new InvalidArgumentException(
                "Invalid value given: '{$value}', reason: " . $dateIntervalException->getMessage(),
                previous: $dateIntervalException,
            );
        }
    }
}

/**
 * @throws InvalidArgumentException
 *
 * @return ($value is null ? Entry<null> : Entry<\DateTimeInterface>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function date_entry(string $name, DateTimeInterface|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new DateEntry($name, null, $metadata);
    }

    if ($value instanceof DateTimeImmutable) {
        return new DateEntry($name, $value->setTime(0, 0, 0, 0), $metadata);
    }

    if ($value instanceof DateTimeInterface) {
        return new DateEntry($name, DateTimeImmutable::createFromInterface($value)->setTime(0, 0, 0, 0), $metadata);
    }

    try {
        return new DateEntry($name, (new DateTimeImmutable($value))->setTime(0, 0, 0, 0), $metadata);
    } catch (Exception $e) {
        throw new InvalidArgumentException(
            "Invalid value given: '{$value}', reason: " . $e->getMessage(),
            previous: $e,
        );
    }
}

/**
 * @return ($value is null ? Entry<null> : Entry<int>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function int_entry(string $name, ?int $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new IntegerEntry($name, null, $metadata);
    }

    return new IntegerEntry($name, $value, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<int>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function integer_entry(string $name, ?int $value, ?Metadata $metadata = null): Entry
{
    return int_entry($name, $value, $metadata);
}

/**
 * @return ($enum is null ? Entry<null> : Entry<\UnitEnum>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function enum_entry(string $name, ?UnitEnum $enum, ?Metadata $metadata = null): Entry
{
    if ($enum === null) {
        return new EnumEntry($name, null, $metadata);
    }

    return new EnumEntry($name, $enum, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<float>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function float_entry(string $name, float|int|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new FloatEntry($name, null, $metadata);
    }

    return new FloatEntry($name, BigDecimal::of((string) $value)->toFloat(), $metadata);
}

/**
 * @param null|array<array-key, mixed>|Json|string $data
 *
 * @throws InvalidArgumentException
 *
 * @return ($data is null ? Entry<null> : Entry<Json>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_entry(string $name, array|string|Json|null $data, ?Metadata $metadata = null): Entry
{
    if ($data === null) {
        return new JsonEntry($name, null, $metadata);
    }

    if ($data instanceof Json) {
        return new JsonEntry($name, $data, $metadata);
    }

    if (is_array($data)) {
        return new JsonEntry($name, Json::fromArray($data), $metadata);
    }

    try {
        return new JsonEntry($name, new Json($data), $metadata);
    } catch (Throwable $e) {
        throw new InvalidArgumentException("Invalid value given: '{$data}', reason: " . $e->getMessage(), previous: $e);
    }
}

/**
 * @param null|array<array-key, mixed>|Json|string $data
 *
 * @throws InvalidArgumentException
 *
 * @return ($data is null ? Entry<null> : Entry<Json>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function json_object_entry(string $name, array|string|Json|null $data, ?Metadata $metadata = null): Entry
{
    if ($data instanceof Json) {
        return new JsonEntry($name, $data, $metadata);
    }

    if (is_string($data)) {
        try {
            return new JsonEntry($name, new Json($data), $metadata);
        } catch (Throwable $e) {
            throw new InvalidArgumentException(
                "Invalid value given: '{$data}', reason: " . $e->getMessage(),
                previous: $e,
            );
        }
    }

    return JsonEntry::object($name, $data, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<string>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function str_entry(string $name, ?string $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new StringEntry($name, null, $metadata);
    }

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
 *
 * @return Entry<?string>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function null_entry(string $name, ?Metadata $metadata = null): Entry
{
    return StringEntry::fromNull($name, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<string>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function string_entry(string $name, ?string $value, ?Metadata $metadata = null): Entry
{
    return str_entry($name, $value, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<\Flow\Types\Value\Uuid>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function uuid_entry(string $name, FlowUuid|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new UuidEntry($name, null, $metadata);
    }

    if ($value instanceof FlowUuid) {
        return new UuidEntry($name, $value, $metadata);
    }

    return new UuidEntry($name, FlowUuid::fromString($value), $metadata);
}

/**
 * @throws InvalidArgumentException
 *
 * @return ($value is null ? Entry<null> : Entry<\DOMDocument|XMLDocument>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_entry(string $name, DOMDocument|XMLDocument|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new XMLEntry($name, null, $metadata);
    }

    if ($value instanceof DOMDocument || $value instanceof XMLDocument) {
        return new XMLEntry($name, $value, $metadata);
    }

    $doc = new DOMDocument();

    if (!@$doc->loadXML($value)) {
        throw new InvalidArgumentException("Given string \"{$value}\" is not valid XML");
    }

    return new XMLEntry($name, $doc, $metadata);
}

/**
 * @throws InvalidArgumentException
 *
 * @return ($value is null ? Entry<null> : Entry<\DOMElement|Element>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function xml_element_entry(string $name, DOMElement|Element|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new XMLElementEntry($name, null, $metadata);
    }

    if ($value instanceof DOMElement || $value instanceof Element) {
        return new XMLElementEntry($name, $value, $metadata);
    }

    $doc = new DOMDocument();

    if (!@$doc->loadXML($value)) {
        throw new InvalidArgumentException("Given string \"{$value}\" is not valid XML");
    }

    $element = $doc->documentElement;

    if (!$element instanceof DOMElement) {
        throw new InvalidArgumentException("Given string \"{$value}\" does not contain a root XML element");
    }

    return new XMLElementEntry($name, $element, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<HTMLDocument>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function html_entry(string $name, HTMLDocument|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new HTMLEntry($name, null, $metadata);
    }

    if ($value instanceof HTMLDocument) {
        return new HTMLEntry($name, $value, $metadata);
    }

    return HTMLEntry::fromString($name, $value, $metadata);
}

/**
 * @return ($value is null ? Entry<null> : Entry<HTMLElement>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function html_element_entry(string $name, HTMLElement|string|null $value, ?Metadata $metadata = null): Entry
{
    if ($value === null) {
        return new HTMLElementEntry($name, null, $metadata);
    }

    if ($value instanceof HTMLElement) {
        return new HTMLElementEntry($name, $value, $metadata);
    }

    return HTMLElementEntry::fromString($name, $value, $metadata);
}

/**
 * @param Entry<mixed> ...$entries
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function entries(Entry ...$entries): Entries
{
    return new Entries(...$entries);
}

/**
 * @template TShape of array<array-key, mixed>
 *
 * @param ?TShape $value
 * @param StructureType<mixed>|Type<TShape> $type
 *
 * @return ($value is null ? Entry<null> : Entry<TShape>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function struct_entry(string $name, ?array $value, Type $type, ?Metadata $metadata = null): Entry
{
    if (!$type instanceof StructureType) {
        // @mago-expect linter:no-fully-qualified-global-function
        throw new InvalidArgumentException(\sprintf(
            'Structure entry "%s" requires a StructureType, got %s',
            $name,
            $type::class,
        ));
    }

    if ($value === null) {
        return new StructureEntry($name, null, $type, $metadata);
    }

    return new StructureEntry($name, $value, $type, $metadata);
}

/**
 * @template TShape of array<array-key, mixed>
 *
 * @param ?TShape $value
 * @param StructureType<mixed>|Type<TShape> $type
 *
 * @return ($value is null ? Entry<null> : Entry<TShape>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function structure_entry(string $name, ?array $value, Type $type, ?Metadata $metadata = null): Entry
{
    if (!$type instanceof StructureType) {
        // @mago-expect linter:no-fully-qualified-global-function
        throw new InvalidArgumentException(\sprintf(
            'Structure entry "%s" requires a StructureType, got %s',
            $name,
            $type::class,
        ));
    }

    if ($value === null) {
        return new StructureEntry($name, null, $type, $metadata);
    }

    return new StructureEntry($name, $value, $type, $metadata);
}

/**
 * @param null|list<mixed> $value
 * @param Type<mixed> $type
 *
 * @return ($value is null ? Entry<null> : Entry<list<mixed>>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function list_entry(string $name, ?array $value, Type $type, ?Metadata $metadata = null): Entry
{
    if (!$type instanceof ListType) {
        // @mago-expect linter:no-fully-qualified-global-function
        throw new InvalidArgumentException(\sprintf(
            'List entry "%s" requires a ListType, got %s',
            $name,
            $type::class,
        ));
    }

    if ($value === null) {
        return new ListEntry($name, null, $type, $metadata);
    }

    return new ListEntry($name, $value, $type, $metadata);
}

/**
 * @param ?array<array-key, mixed> $value
 * @param Type<mixed> $mapType
 *
 * @return ($value is null ? Entry<null> : Entry<array<array-key, mixed>>)
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::ENTRY)]
function map_entry(string $name, ?array $value, Type $mapType, ?Metadata $metadata = null): Entry
{
    if (!$mapType instanceof MapType) {
        // @mago-expect linter:no-fully-qualified-global-function
        throw new InvalidArgumentException(\sprintf(
            'Map entry "%s" requires a MapType, got %s',
            $name,
            $mapType::class,
        ));
    }

    if ($value === null) {
        return new MapEntry($name, null, $mapType, $metadata);
    }

    return new MapEntry($name, $value, $mapType, $metadata);
}

/**
 * @param Entry<mixed> ...$entry
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function row(Entry ...$entry): Row
{
    return Row::create(...$entry);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function rows(Row ...$row): Rows
{
    return new Rows(...$row);
}

/**
 * @param array<Row> $rows
 * @param array<Partition|string>|Partitions $partitions
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function rows_partitioned(array $rows, array|Partitions $partitions): Rows
{
    return Rows::partitioned($rows, $partitions);
}

/**
 * An alias for `ref`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function col(string $entry): EntryReference
{
    return new EntryReference($entry);
}

/**
 * An alias for `ref`.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
#[DocumentationExample(topic: 'data_frame', example: 'columns', option: 'create')]
function entry(string $entry): EntryReference
{
    return new EntryReference($entry);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
#[DocumentationExample(topic: 'data_frame', example: 'columns', option: 'create')]
function ref(string $entry): EntryReference
{
    return new EntryReference($entry);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function structure_ref(string $entry): StructureFunctions
{
    return ref($entry)->structure();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function list_ref(string $entry): ListFunctions
{
    return ref($entry)->list();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function refs(string|Reference ...$entries): References
{
    return new References(...$entries);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function select(string|Reference ...$entries): Select
{
    return new Select(...$entries);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function drop(string|Reference ...$entries): Drop
{
    return new Drop(...$entries);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function add_row_index(string $column = 'index', StartFrom $startFrom = StartFrom::ZERO): AddRowIndex
{
    return new AddRowIndex($column, $startFrom);
}

/**
 * @param int<1, max> $size
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::TRANSFORMER)]
function batch_size(int $size): BatchSize
{
    return new BatchSize($size);
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
#[DocumentationExample(topic: 'data_frame', example: 'columns', option: 'create')]
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

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get(ScalarFunction $ref, ScalarFunction|string $path): ArrayGet
{
    return new ArrayGet($ref, $path);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $keys
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_get_collection(ScalarFunction $ref, ScalarFunction|array $keys): ArrayGetCollection
{
    return new ArrayGetCollection($ref, $keys);
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
function array_key_rename(
    ScalarFunction $ref,
    ScalarFunction|string $path,
    ScalarFunction|string $newName,
): ArrayKeyRename {
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
    ScalarFunction|Sort|null $sort_function = null,
    ScalarFunction|int|null $flags = null,
    ScalarFunction|bool $recursive = true,
): ArraySort {
    if ($sort_function === null) {
        $sort_function = Sort::sort;
    }

    return new ArraySort($function, $sort_function, $flags, $recursive);
}

/**
 * @param array<array-key, mixed>|ScalarFunction $function
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_reverse(ScalarFunction|array $function, ScalarFunction|bool $preserveKeys = false): ArrayReverse
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

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function count(?EntryReference $function = null): Count
{
    return new Count($function);
}

/**
 * Calls a user-defined function with the given parameters.
 *
 * @param callable|ScalarFunction $callable
 * @param array<mixed> $parameters
 * @param null|Type<mixed> $return_type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function call(ScalarFunction|callable $callable, array $parameters = [], ?Type $return_type = null): CallUserFunc
{
    return new CallUserFunc($callable, $parameters, $return_type);
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
/**
 * @param array<array-key, mixed>|ScalarFunction $array
 * @param array<array-key, mixed>|ScalarFunction $skip_keys
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function array_unpack(
    ScalarFunction|array $array,
    ScalarFunction|array $skip_keys = [],
    ScalarFunction|string|null $entry_prefix = null,
): ArrayUnpack {
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
function uuid_v7(ScalarFunction|DateTimeInterface|null $value = null): Uuid
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
function to_timezone(ScalarFunction|DateTimeInterface $value, ScalarFunction|DateTimeZone|string $timeZone): ToTimeZone
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
    ScalarFunction|int $flags = 0,
    ScalarFunction|int $offset = 0,
): Regex {
    return new Regex($pattern, $subject, $flags, $offset);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCALAR_FUNCTION)]
function regex_all(
    ScalarFunction|string $pattern,
    ScalarFunction|string $subject,
    ScalarFunction|int $flags = 0,
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
 * @param array<mixed> $data
 *
 * @return Entry<mixed>
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function to_entry(string $name, mixed $data, EntryFactory $entryFactory): Entry
{
    return $entryFactory->create($name, $data);
}

/**
 * @param array<array<mixed>>|array<mixed|string> $data
 * @param array<Partition>|Partitions $partitions
 * @param null|Schema $schema
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function array_to_row(
    array $data,
    EntryFactory $entryFactory,
    array|Partitions $partitions = [],
    ?Schema $schema = null,
): Row {
    $entries = [];

    // @mago-ignore analysis:mixed-assignment
    foreach ($data as $key => $value) {
        $name = is_int($key) ? 'e' . str_pad((string) $key, 2, '0', STR_PAD_LEFT) : $key;

        try {
            $entries[$name] = $entryFactory->create($name, $value, $schema);
        } catch (SchemaDefinitionNotFoundException $e) {
            if ($schema === null) {
                throw $e;
            }
        }
    }

    foreach ($partitions as $partition) {
        if (!array_key_exists($partition->name, $entries)) {
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
            if (!array_key_exists($definition->entry()->name(), $entries)) {
                $entries[$definition->entry()->name()] = str_entry($definition->entry()->name(), null);
            }
        }
    }

    return Row::create(...array_values($entries));
}

/**
 * @param array<array<mixed>>|array<mixed|string> $data
 * @param array<Partition>|Partitions $partitions
 * @param null|Schema $schema
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function array_to_rows(
    array $data,
    EntryFactory $entryFactory,
    array|Partitions $partitions = [],
    ?Schema $schema = null,
): Rows {
    $partitions = is_array($partitions) ? new Partitions(...$partitions) : $partitions;

    $isRows = true;

    // @mago-ignore analysis:mixed-assignment
    foreach ($data as $v) {
        if (!is_array($v)) {
            $isRows = false;

            break;
        }
    }

    if (!$isRows) {
        return Rows::partitioned([array_to_row($data, $entryFactory, $partitions, $schema)], $partitions);
    }

    $rows = [];

    // @mago-ignore analysis:mixed-assignment
    foreach ($data as $row) {
        $row = type_array()->assert($row);
        $rows[] = array_to_row($row, $entryFactory, $partitions, $schema);
    }

    return Rows::partitioned($rows, $partitions);
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
function average(EntryReference|string $ref, int $scale = 2, Rounding $rounding = Rounding::HALF_UP): Average
{
    return new Average(is_string($ref) ? ref($ref) : $ref, $scale, $rounding);
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
function collect(EntryReference|string $ref): Collect
{
    return new Collect(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function string_agg(EntryReference|string $ref, string $separator = ', ', ?SortOrder $sort = null): StringAggregate
{
    return new StringAggregate(is_string($ref) ? ref($ref) : $ref, $separator, $sort);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function collect_unique(EntryReference|string $ref): CollectUnique
{
    return new CollectUnique(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function window(): Window
{
    return new Window();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function sum(EntryReference|string $ref): Sum
{
    return new Sum(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function first(EntryReference|string $ref): First
{
    return new First(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function last(EntryReference|string $ref): Last
{
    return new Last(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function max(EntryReference|string $ref): Max
{
    return new Max(is_string($ref) ? ref($ref) : $ref);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::AGGREGATING_FUNCTION)]
function min(EntryReference|string $ref): Min
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
function null_schema(string $name, ?Metadata $metadata = null): StringDefinition
{
    return new StringDefinition(
        $name,
        true,
        Metadata::fromArray([Metadata::FROM_NULL => true])->merge($metadata ?? Metadata::empty()),
    );
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function datetime_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): DateTimeDefinition
{
    return new DateTimeDefinition($name, $nullable, $metadata);
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
 * @param StructureType<T>|Type<array<string, T>> $type
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
    /** @var StructureType<T> $type */
    return new StructureDefinition($name, $type, $nullable, $metadata);
}

/**
 * @param Type<mixed>|UnionType<mixed, mixed> $type
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function union_schema(
    string $name,
    UnionType|Type $type,
    bool $nullable = false,
    ?Metadata $metadata = null,
): UnionDefinition {
    /** @var UnionType<mixed, mixed> $type */
    return new UnionDefinition($name, $type, $nullable, $metadata);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::SCHEMA)]
function uuid_schema(string $name, bool $nullable = false, ?Metadata $metadata = null): UuidDefinition
{
    return new UuidDefinition($name, $nullable, $metadata);
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
    if (!array_key_exists('ref', $definition) || !is_string($definition['ref'])) {
        throw new RuntimeException('Definition array must have a string "ref" key');
    }

    if (!array_key_exists('type', $definition) || !is_array($definition['type'])) {
        throw new RuntimeException('Definition array must have an array "type" key');
    }

    $ref = $definition['ref'];

    /** @var array<string, mixed> $typeData */
    $typeData = $definition['type'];
    $type = TypeFactory::fromArray($typeData);

    $nullable = isset($definition['nullable']) && is_bool($definition['nullable']) ? $definition['nullable'] : false;

    /** @var array<string, array<mixed>|bool|float|int|string> $metadataData */
    $metadataData = isset($definition['metadata']) && is_array($definition['metadata']) ? $definition['metadata'] : [];
    $metadata = Metadata::fromArray($metadataData);

    return definition_from_type($ref, $type, $nullable, $metadata);
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
    return match (true) {
        $type instanceof BooleanType => new BooleanDefinition($ref, $nullable, $metadata),
        $type instanceof IntegerType => new IntegerDefinition($ref, $nullable, $metadata),
        $type instanceof FloatType => new FloatDefinition($ref, $nullable, $metadata),
        $type instanceof StringType => new StringDefinition($ref, $nullable, $metadata),
        $type instanceof DateType => new DateDefinition($ref, $nullable, $metadata),
        $type instanceof DateTimeType => new DateTimeDefinition($ref, $nullable, $metadata),
        $type instanceof TimeType => new TimeDefinition($ref, $nullable, $metadata),
        $type instanceof JsonType => new JsonDefinition($ref, $nullable, $metadata),
        $type instanceof ArrayType => new JsonDefinition($ref, $nullable, $metadata),
        $type instanceof UuidType => new UuidDefinition($ref, $nullable, $metadata),
        $type instanceof ListType => new ListDefinition($ref, $type, $nullable, $metadata),
        $type instanceof MapType => new MapDefinition($ref, $type, $nullable, $metadata),
        $type instanceof StructureType => new StructureDefinition($ref, $type, $nullable, $metadata),
        $type instanceof UnionType => new UnionDefinition($ref, $type, $nullable, $metadata),
        $type instanceof EnumType => new EnumDefinition($ref, $type->class, $nullable, $metadata),
        $type instanceof HTMLType => new HTMLDefinition($ref, $nullable, $metadata),
        $type instanceof HTMLElementType => new HTMLElementDefinition($ref, $nullable, $metadata),
        $type instanceof XMLType => new XMLDefinition($ref, $nullable, $metadata),
        $type instanceof XMLElementType => new XMLElementDefinition($ref, $nullable, $metadata),
        // @mago-expect linter:no-fully-qualified-global-function
        default => throw new RuntimeException(\sprintf('Cannot create Definition from type: %s', $type::class)),
    };
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function execution_context(?Config $config = null): FlowContext
{
    return new FlowContext($config ?? Config::default());
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function flow_context(?Config $config = null): FlowContext
{
    return execution_context($config);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config(): Config
{
    return Config::default();
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function config_builder(): ConfigBuilder
{
    return new ConfigBuilder();
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

/**
 * In this mode, functions throws exceptions if the given entry is not found
 * or passed parameters are invalid.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function execution_strict(): ExecutionMode
{
    return ExecutionMode::STRICT;
}

/**
 * In this mode, functions returns nulls instead of throwing exceptions.
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function execution_lenient(): ExecutionMode
{
    return ExecutionMode::LENIENT;
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
function compare_all(Comparison $comparison, Comparison ...$comparisons): Comparison\All
{
    return new Comparison\All($comparison, ...$comparisons);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::COMPARISON)]
function compare_any(Comparison $comparison, Comparison ...$comparisons): Comparison\Any
{
    return new Comparison\Any($comparison, ...$comparisons);
}

/**
 * @param array<Comparison|string>|Comparison $comparisons
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
#[DocumentationExample(topic: 'join', example: 'join')]
#[DocumentationExample(topic: 'join', example: 'join_each')]
function join_on(array|Comparison $comparisons, string $join_prefix = ''): Expression
{
    return Expression::on($comparisons, $join_prefix);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_name(Order $order = Order::ASC): Comparator
{
    return new NameComparator($order);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_name_desc(): Comparator
{
    return new NameComparator(Order::DESC);
}

/**
 * @param array<class-string<Entry<mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type(array $priorities = TypePriorities::PRIORITIES, Order $order = Order::ASC): Comparator
{
    return new TypeComparator(new TypePriorities($priorities), $order);
}

/**
 * @param array<class-string<Entry<mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type_desc(array $priorities = TypePriorities::PRIORITIES): Comparator
{
    return new TypeComparator(new TypePriorities($priorities), Order::DESC);
}

/**
 * @param array<class-string<Entry<mixed>>, int> $priorities
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::DATA_FRAME)]
function compare_entries_by_type_and_name(
    array $priorities = TypePriorities::PRIORITIES,
    Order $order = Order::ASC,
): Comparator {
    return new CombinedComparator(
        new TypeComparator(new TypePriorities($priorities), $order),
        new NameComparator($order),
    );
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
function with_entry(string $name, ScalarFunction $function): WithEntry
{
    return new WithEntry($name, $function);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function constraint_unique(string $reference, string ...$references): UniqueConstraint
{
    return new UniqueConstraint($reference, ...$references);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function constraint_sorted_by(string|Reference $column, string|Reference ...$columns): SortedByConstraint
{
    $references = array_map(static fn(string|Reference $ref) => EntryReference::init($ref), [$column, ...$columns]);

    return new SortedByConstraint(...$references);
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
function retry_any_throwable(int $limit): AnyThrowable
{
    return new AnyThrowable($limit);
}

/**
 * @param array<class-string<\Throwable>> $exception_types
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function retry_on_exception_types(array $exception_types, int $limit): OnExceptionTypes
{
    return new OnExceptionTypes($exception_types, $limit);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function delay_linear(Duration $delay, Duration $increment): Linear
{
    return new Linear($delay, $increment);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function delay_exponential(Duration $base, int $multiplier = 2, ?Duration $max_delay = null): Exponential
{
    return new Exponential($base, $multiplier, $max_delay);
}

/**
 * @param float $jitter_factor a value between 0 and 1 representing the maximum percentage of jitter to apply
 */
#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function delay_jitter(DelayFactory $delay, float $jitter_factor): Jitter
{
    return new Jitter($delay, $jitter_factor);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function delay_fixed(Duration $delay): Fixed
{
    return new Fixed($delay);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function duration_seconds(int $seconds): Duration
{
    return Duration::fromSeconds($seconds);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function duration_milliseconds(int $milliseconds): Duration
{
    return Duration::fromMilliseconds($milliseconds);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function duration_microseconds(int $microseconds): Duration
{
    return Duration::fromMicroseconds($microseconds);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function duration_minutes(int $minutes): Duration
{
    return Duration::fromMinutes($minutes);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::LOADER)]
function write_with_retries(
    Loader $loader,
    RetryStrategy $retry_strategy = new AnyThrowable(3),
    DelayFactory $delay_factory = new FixedMilliseconds(200),
    Sleep $sleep = new SystemSleep(),
): RetryLoader {
    return new RetryLoader($loader, $retry_strategy, $delay_factory, $sleep);
}

#[DocumentationDSL(module: Module::CORE, type: DSLType::HELPER)]
function clock(string $time_zone = 'UTC'): ClockInterface
{
    return new SystemClock(new DateTimeZone($time_zone));
}
