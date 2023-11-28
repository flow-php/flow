<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Types\Type;
use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Adapter\Avro\FlixTech\AvroLoader;
use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Adapter\ChartJS\Chart\BarChart;
use Flow\ETL\Adapter\ChartJS\Chart\LineChart;
use Flow\ETL\Adapter\ChartJS\Chart\PieChart;
use Flow\ETL\Adapter\ChartJS\ChartJSLoader;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\ETL\Adapter\Doctrine\DbalDataFrameFactory;
use Flow\ETL\Adapter\Doctrine\DbalLimitOffsetExtractor;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\OrderBy;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Doctrine\QueryParameter;
use Flow\ETL\Adapter\Doctrine\Table;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\DocumentDataSource;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchExtractor;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchLoader;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\HitsIntoRowsTransformer;
use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Adapter\GoogleSheet\Columns;
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor;
use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchExtractor;
use Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchLoader;
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\ParquetLoader;
use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Adapter\Text\TextLoader;
use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Config;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\ErrorHandler\SkipRows;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Flow;
use Flow\ETL\Formatter;
use Flow\ETL\Function\All;
use Flow\ETL\Function\Any;
use Flow\ETL\Function\ArrayExists;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArrayGet;
use Flow\ETL\Function\ArrayGetCollection;
use Flow\ETL\Function\ArrayKeyRename;
use Flow\ETL\Function\ArrayKeysStyleConvert;
use Flow\ETL\Function\ArrayMerge;
use Flow\ETL\Function\ArrayMergeCollection;
use Flow\ETL\Function\ArrayReverse;
use Flow\ETL\Function\ArraySort;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\ArrayUnpack;
use Flow\ETL\Function\Average;
use Flow\ETL\Function\CallMethod;
use Flow\ETL\Function\Capitalize;
use Flow\ETL\Function\Cast;
use Flow\ETL\Function\Collect;
use Flow\ETL\Function\CollectUnique;
use Flow\ETL\Function\Combine;
use Flow\ETL\Function\Concat;
use Flow\ETL\Function\Count;
use Flow\ETL\Function\DateTimeFormat;
use Flow\ETL\Function\DensRank;
use Flow\ETL\Function\Exists;
use Flow\ETL\Function\First;
use Flow\ETL\Function\Hash;
use Flow\ETL\Function\Last;
use Flow\ETL\Function\Literal;
use Flow\ETL\Function\Max;
use Flow\ETL\Function\Min;
use Flow\ETL\Function\Not;
use Flow\ETL\Function\Now;
use Flow\ETL\Function\NumberFormat;
use Flow\ETL\Function\Optional;
use Flow\ETL\Function\PregMatch;
use Flow\ETL\Function\PregMatchAll;
use Flow\ETL\Function\PregReplace;
use Flow\ETL\Function\Rank;
use Flow\ETL\Function\Round;
use Flow\ETL\Function\RowNumber;
use Flow\ETL\Function\Sanitize;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\Size;
use Flow\ETL\Function\Split;
use Flow\ETL\Function\Sprintf;
use Flow\ETL\Function\StyleConverter\StringStyles;
use Flow\ETL\Function\Sum;
use Flow\ETL\Function\ToDate;
use Flow\ETL\Function\ToDateTime;
use Flow\ETL\Function\ToLower;
use Flow\ETL\Function\ToMoney;
use Flow\ETL\Function\ToTimeZone;
use Flow\ETL\Function\ToUpper;
use Flow\ETL\Function\Ulid;
use Flow\ETL\Function\Uuid;
use Flow\ETL\Function\When;
use Flow\ETL\Loader;
use Flow\ETL\Loader\CallbackLoader;
use Flow\ETL\Loader\MemoryLoader;
use Flow\ETL\Loader\StreamLoader;
use Flow\ETL\Loader\StreamLoader\Output;
use Flow\ETL\Loader\TransformerLoader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Partition;
use Flow\ETL\Pipeline;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\Schema\Formatter\ASCIISchemaFormatter;
use Flow\ETL\Row\Schema\SchemaFormatter;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Window;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Google\Client;
use Google\Service\Sheets;
use Psr\Http\Client\ClientInterface;

function read(Extractor $extractor, Config|ConfigBuilder|null $config = null) : DataFrame
{
    return (new Flow($config))->from($extractor);
}

function from_rows(Rows ...$rows) : Extractor\ProcessExtractor
{
    return new Extractor\ProcessExtractor(...$rows);
}

function from_array(array $array) : Extractor\MemoryExtractor
{
    return new Extractor\MemoryExtractor(new ArrayMemory($array));
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
    /** @psalm-suppress ArgumentTypeCoercion */
    return new Extractor\SequenceExtractor(
        new Extractor\SequenceGenerator\DatePeriodSequenceGenerator(new \DatePeriod($start, $interval, $end, $options)),
        $entry_name
    );
}

function from_sequence_date_period_recurrences(string $entry_name, \DateTimeInterface $start, \DateInterval $interval, int $recurrences, int $options = 0) : Extractor\SequenceExtractor
{
    /** @psalm-suppress ArgumentTypeCoercion */
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
    return new StreamLoader($uri, Mode::from($mode), $truncate, $output, $formatter, $schemaFormatter);
}

function to_transformation(Transformer $transformer, Loader $loader) : TransformerLoader
{
    return new TransformerLoader($transformer, $loader);
}

function row(Row\Entry ...$entry) : Row
{
    return Row::create(...$entry);
}

function rows(Row ...$row) : Rows
{
    return new Rows(...$row);
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

function refs(string|Reference ...$entries) : References
{
    return new References(...$entries);
}

function optional(ScalarFunction $function) : ScalarFunction
{
    return new Optional($function);
}

function lit(mixed $value) : ScalarFunction
{
    return new Literal($value);
}

function exists(ScalarFunction $ref) : ScalarFunction
{
    return new Exists($ref);
}

function when(ScalarFunction $ref, ScalarFunction $then, ?ScalarFunction $else = null) : ScalarFunction
{
    return new When($ref, $then, $else);
}

function array_get(ScalarFunction $ref, string $path) : ScalarFunction
{
    return new ArrayGet($ref, $path);
}

function array_get_collection(ScalarFunction $ref, string ...$keys) : ScalarFunction
{
    return new ArrayGetCollection($ref, $keys);
}

function array_get_collection_first(ScalarFunction $ref, string ...$keys) : ScalarFunction
{
    return ArrayGetCollection::fromFirst($ref, $keys);
}

function array_exists(ScalarFunction $ref, string $path) : ScalarFunction
{
    return new ArrayExists($ref, $path);
}

function array_merge(ScalarFunction $left, ScalarFunction $right) : ScalarFunction
{
    return new ArrayMerge($left, $right);
}

function array_merge_collection(ScalarFunction $ref) : ScalarFunction
{
    return new ArrayMergeCollection($ref);
}

function array_key_rename(ScalarFunction $ref, string $path, string $newName) : ScalarFunction
{
    return new ArrayKeyRename($ref, $path, $newName);
}

function array_keys_style_convert(ScalarFunction $ref, StringStyles|string $style = StringStyles::SNAKE) : ScalarFunction
{
    return new ArrayKeysStyleConvert($ref, $style instanceof StringStyles ? $style : StringStyles::fromString($style));
}

function array_sort(ScalarFunction $function, ?string $sort_function = null, ?int $flags = null, bool $recursive = true) : ScalarFunction
{
    return new ArraySort($function, $sort_function ? Sort::fromString($sort_function) : Sort::sort, $flags, $recursive);
}

function array_reverse(ScalarFunction $function, bool $preserveKeys = false) : ScalarFunction
{
    return new ArrayReverse($function, $preserveKeys);
}

function now(\DateTimeZone $time_zone = new \DateTimeZone('UTC')) : ScalarFunction
{
    return new Now($time_zone);
}

function to_date_time(ScalarFunction $ref, string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ScalarFunction
{
    return new ToDateTime($ref, $format, $timeZone);
}

function to_date(ScalarFunction $ref, string $format = 'Y-m-d', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ScalarFunction
{
    return new ToDate($ref, $format, $timeZone);
}

function date_time_format(ScalarFunction $ref, string $format) : ScalarFunction
{
    return new DateTimeFormat($ref, $format);
}

/**
 * @param non-empty-string $separator
 */
function split(ScalarFunction $ref, string $separator, int $limit = PHP_INT_MAX) : ScalarFunction
{
    return new Split($ref, $separator, $limit);
}

function combine(ScalarFunction $keys, ScalarFunction $values) : ScalarFunction
{
    return new Combine($keys, $values);
}

function concat(ScalarFunction ...$functions) : ScalarFunction
{
    return new Concat(...$functions);
}

function hash(ScalarFunction $function, string $algorithm = 'xxh128', bool $binary = false, array $options = []) : ScalarFunction
{
    return new Hash($function, $algorithm, $binary, $options);
}

function cast(ScalarFunction $function, string $type) : ScalarFunction
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
function array_unpack(ScalarFunction $function, array $skip_keys = [], ?string $entry_prefix = null) : ScalarFunction
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
function array_expand(ScalarFunction $function, ArrayExpand $expand = ArrayExpand::VALUES) : ScalarFunction
{
    return new \Flow\ETL\Function\ArrayExpand($function, $expand);
}

function size(ScalarFunction $function) : ScalarFunction
{
    return new Size($function);
}

function uuid_v4() : ScalarFunction
{
    return Uuid::uuid4();
}

function uuid_v7(?ScalarFunction $function = null) : ScalarFunction
{
    return Uuid::uuid7($function);
}

function ulid(?ScalarFunction $function = null) : ScalarFunction
{
    return new Ulid($function);
}

function lower(ScalarFunction $function) : ScalarFunction
{
    return new ToLower($function);
}

function capitalize(ScalarFunction $function) : ScalarFunction
{
    return new Capitalize($function);
}

function upper(ScalarFunction $function) : ScalarFunction
{
    return new ToUpper($function);
}

function call_method(ScalarFunction $object, ScalarFunction $method, ScalarFunction ...$params) : ScalarFunction
{
    return new CallMethod($object, $method, ...$params);
}

function all(ScalarFunction ...$functions) : ScalarFunction
{
    return new All(...$functions);
}

function any(ScalarFunction ...$functions) : ScalarFunction
{
    return new Any(...$functions);
}

function not(ScalarFunction $function) : ScalarFunction
{
    return new Not($function);
}

function to_timezone(ScalarFunction $function, ScalarFunction $timeZone) : ScalarFunction
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

function to_money(ScalarFunction $amount, ScalarFunction $currency, ?\Money\MoneyParser $moneyParser = null) : ScalarFunction
{
    if (null !== $moneyParser) {
        return new ToMoney($amount, $currency, $moneyParser);
    }

    return new ToMoney($amount, $currency);
}

function regex_replace(ScalarFunction $pattern, ScalarFunction $replacement, ScalarFunction $subject) : ScalarFunction
{
    return new PregReplace($pattern, $replacement, $subject);
}

function regex_match_all(ScalarFunction $pattern, ScalarFunction $subject, ?ScalarFunction $flags = null) : ScalarFunction
{
    return new PregMatchAll($pattern, $subject, $flags);
}

function regex_match(ScalarFunction $pattern, ScalarFunction $subject) : ScalarFunction
{
    return new PregMatch($pattern, $subject);
}

function sprintf(ScalarFunction $format, ScalarFunction ...$args) : ScalarFunction
{
    return new Sprintf($format, ...$args);
}

function sanitize(ScalarFunction $function, ?ScalarFunction $placeholder = null, ?ScalarFunction $skipCharacters = null) : ScalarFunction
{
    return new Sanitize($function, $placeholder ?: new Literal('*'), $skipCharacters ?: new Literal(0));
}

/**
 * @param ScalarFunction $function
 * @param null|ScalarFunction $precision
 * @param int<0, max> $mode
 *
 * @return ScalarFunction
 */
function round(ScalarFunction $function, ?ScalarFunction $precision = null, int $mode = PHP_ROUND_HALF_UP) : ScalarFunction
{
    return new Round($function, $precision ?? lit(2), $mode);
}

function number_format(ScalarFunction $function, ?ScalarFunction $decimals = null, ?ScalarFunction $decimalSeparator = null, ?ScalarFunction $thousandsSeparator = null) : ScalarFunction
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
 * @param array<Partition> $partitions
 */
function array_to_rows(array $data, EntryFactory $entryFactory = new NativeEntryFactory(), array $partitions = []) : Rows
{
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
            $entries[] = $entryFactory->create(\is_int($key) ? 'e' . \str_pad((string) $key, 2, '0', STR_PAD_LEFT) : $key, $value);
        }

        return \count($partitions)
            ? Rows::partitioned([Row::create(...$entries)], $partitions)
            : new Rows(Row::create(...$entries));
    }
    $rows = [];

    foreach ($data as $row) {
        $entries = [];

        foreach ($row as $column => $value) {
            $entries[] = $entryFactory->create(\is_int($column) ? 'e' . \str_pad((string) $column, 2, '0', STR_PAD_LEFT) : $column, $value);
        }
        $rows[] = Row::create(...$entries);
    }

    return \count($partitions)
        ? Rows::partitioned($rows, $partitions)
        : new Rows(...$rows);
}

function rank() : Rank
{
    return new Rank();
}

function dens_rank() : DensRank
{
    return new DensRank();
}

function average(Reference $ref) : Average
{
    return new Average($ref);
}

function collect(Reference $ref) : Collect
{
    return new Collect($ref);
}

function collect_unique(Reference $ref) : CollectUnique
{
    return new CollectUnique($ref);
}

function window() : Window
{
    return new Window();
}

function sum(Reference $ref) : Sum
{
    return new Sum($ref);
}

function first(Reference $ref) : First
{
    return new First($ref);
}

function last(Reference $ref) : Last
{
    return new Last($ref);
}

function max(Reference $ref) : Max
{
    return new Max($ref);
}

function min(Reference $ref) : Min
{
    return new Min($ref);
}

function row_number() : RowNumber
{
    return new RowNumber();
}

if (\class_exists('\Flow\ETL\Adapter\Avro\FlixTech\AvroLoader')) {
    function from_avro(Path|string|array $path) : Extractor
    {
        if (\is_array($path)) {
            /** @var array<Extractor> $extractors */
            $extractors = [];

            foreach ($path as $next_path) {
                $extractors[] = new AvroExtractor(
                    \is_string($next_path) ? Path::realpath($next_path) : $next_path,
                );
            }

            return from_all(...$extractors);
        }

        return new AvroExtractor(
            \is_string($path) ? Path::realpath($path) : $path
        );
    }

    function to_avro(Path|string $path, ?Schema $schema = null) : AvroLoader
    {
        return new AvroLoader(
            \is_string($path) ? Path::realpath($path) : $path,
            $schema
        );
    }
}

if (\class_exists('\Flow\ETL\Adapter\ChartJS\ChartJSLoader')) {
    function bar_chart(EntryReference $label, References $datasets) : BarChart
    {
        return new BarChart($label, $datasets);
    }

    function line_chart(EntryReference $label, References $datasets) : LineChart
    {
        return new LineChart($label, $datasets);
    }

    function pie_chart(EntryReference $label, References $datasets) : PieChart
    {
        return new PieChart($label, $datasets);
    }

    function to_chartjs_file(Chart $type, Path|string|null $output = null, Path|string|null $template = null) : ChartJSLoader
    {
        if (\is_string($output)) {
            $output = Path::realpath($output);
        }

        if (null === $template) {
            return new ChartJSLoader($type, $output);
        }

        if (\is_string($template)) {
            $template = Path::realpath($template);
        }

        return new ChartJSLoader($type, output: $output, template: $template);
    }

    function to_chartjs_var(Chart $type, array &$output) : ChartJSLoader
    {
        /** @psalm-suppress ReferenceConstraintViolation */
        return new ChartJSLoader($type, outputVar: $output);
    }
}

if (\class_exists('\Flow\ETL\Adapter\CSV\CSVExtractor')) {
    /**
     * @param int<0, max> $characters_read_in_line
     */
    function from_csv(
        string|Path|array $path,
        bool $with_header = true,
        bool $empty_to_null = true,
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\',
        int $characters_read_in_line = 1000
    ) : Extractor {
        if (\is_array($path)) {
            $extractors = [];

            foreach ($path as $file_path) {
                $extractors[] = new CSVExtractor(
                    \is_string($file_path) ? Path::realpath($file_path) : $file_path,
                    $with_header,
                    $empty_to_null,
                    $delimiter,
                    $enclosure,
                    $escape,
                    $characters_read_in_line,
                );
            }

            return from_all(...$extractors);
        }

        return new CSVExtractor(
            \is_string($path) ? Path::realpath($path) : $path,
            $with_header,
            $empty_to_null,
            $delimiter,
            $enclosure,
            $escape,
            $characters_read_in_line,
        );
    }

    function to_csv(
        string|Path $uri,
        bool $with_header = true,
        string $separator = ',',
        string $enclosure = '"',
        string $escape = '\\',
        string $new_line_separator = PHP_EOL
    ) : Loader {
        return new CSVLoader(
            \is_string($uri) ? Path::realpath($uri) : $uri,
            $with_header,
            $separator,
            $enclosure,
            $escape,
            $new_line_separator
        );
    }
}

if (\class_exists('\Flow\ETL\Adapter\Doctrine\DbalLimitOffsetExtractor')) {
    /**
     * @param array<string, mixed>|Connection $connection
     * @param string $query
     * @param QueryParameter ...$parameters
     *
     * @return DataFrameFactory
     */
    function dbal_dataframe_factory(
        array|Connection $connection,
        string $query,
        QueryParameter ...$parameters
    ) : DataFrameFactory {
        return \is_array($connection)
            ? new DbalDataFrameFactory($connection, $query, ...$parameters)
            : DbalDataFrameFactory::fromConnection($connection, $query, ...$parameters);
    }

    /**
     * @param Connection $connection
     * @param string|Table $table
     * @param array<OrderBy>|OrderBy $order_by
     * @param int $page_size
     * @param null|int $maximum
     *
     * @throws InvalidArgumentException
     *
     * @return Extractor
     */
    function from_dbal_limit_offset(
        Connection $connection,
        string|Table $table,
        array|OrderBy $order_by,
        int $page_size = 1000,
        ?int $maximum = null,
    ) : Extractor {
        return DbalLimitOffsetExtractor::table(
            $connection,
            \is_string($table) ? new Table($table) : $table,
            $order_by instanceof OrderBy ? [$order_by] : $order_by,
            $page_size,
            $maximum,
        );
    }

    /**
     * @param Connection $connection
     * @param int $page_size
     * @param null|int $maximum
     *
     * @return Extractor
     */
    function from_dbal_limit_offset_qb(
        Connection $connection,
        QueryBuilder $queryBuilder,
        int $page_size = 1000,
        ?int $maximum = null,
    ) : Extractor {
        return new DbalLimitOffsetExtractor(
            $connection,
            $queryBuilder,
            $page_size,
            $maximum,
        );
    }

    /**
     * @param Connection $connection
     * @param string $query
     * @param null|ParametersSet $parameters_set - each one parameters array will be evaluated as new query
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     *
     * @return Extractor
     */
    function dbal_from_queries(
        Connection $connection,
        string $query,
        ?ParametersSet $parameters_set = null,
        array $types = [],
    ) : Extractor {
        return new DbalQueryExtractor(
            $connection,
            $query,
            $parameters_set,
            $types,
        );
    }

    /**
     * @param Connection $connection
     * @param string $query
     * @param array<string, mixed>|list<mixed> $parameters
     * @param array<int, null|int|string|Type>|array<string, null|int|string|Type> $types
     *
     * @return Extractor
     */
    function dbal_from_query(
        Connection $connection,
        string $query,
        array $parameters = [],
        array $types = [],
    ) : Extractor {
        return DbalQueryExtractor::single(
            $connection,
            $query,
            $parameters,
            $types,
        );
    }

    /**
     * In order to control the size of the single insert, use DataFrame::chunkSize() method just before calling DataFrame::load().
     *
     * @param array<string, mixed>|Connection $connection
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $options
     *
     * @throws InvalidArgumentException
     */
    function to_dbal_table_insert(
        array|Connection $connection,
        string $table,
        array $options = [],
    ) : Loader {
        return \is_array($connection)
            ? new DbalLoader($table, $connection, $options, 'insert')
            : DbalLoader::fromConnection($connection, $table, $options, 'insert');
    }

    /**
     *  In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
     *
     * @param array<string, mixed>|Connection $connection
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>,
     *  primary_key_columns?: array<string>
     * } $options
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    function to_dbal_table_update(
        array|Connection $connection,
        string $table,
        array $options = [],
    ) : Loader {
        return \is_array($connection)
            ? new DbalLoader($table, $connection, $options, 'update')
            : DbalLoader::fromConnection($connection, $table, $options, 'update');
    }
}

if (\class_exists('\Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchExtractor')) {
    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.
     *
     * In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
     *
     * @param array{
     *  hosts?: array<string>,
     *  connectionParams?: array<mixed>,
     *  retries?: int,
     *  sniffOnStart?: boolean,
     *  sslCert?: array<string>,
     *  sslKey?: array<string>,
     *  sslVerification?: boolean|string,
     *  elasticMetaHeader?: boolean,
     *  includePortInHostHeader?: boolean
     * } $config
     * @param string $index
     * @param IdFactory $id_factory
     * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
     *
     * @return ElasticsearchLoader
     */
    function to_es_bulk_index(
        array $config,
        string $index,
        IdFactory $id_factory,
        array $parameters = []
    ) : ElasticsearchLoader {
        return new ElasticsearchLoader($config, $index, $id_factory, $parameters);
    }

    /**
     *  https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.
     *
     * In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
     *
     * @param array{
     *  hosts?: array<string>,
     *  connectionParams?: array<mixed>,
     *  retries?: int,
     *  sniffOnStart?: boolean,
     *  sslCert?: array<string>,
     *  sslKey?: array<string>,
     *  sslVerification?: boolean|string,
     *  elasticMetaHeader?: boolean,
     *  includePortInHostHeader?: boolean
     * } $config
     * @param string $index
     * @param IdFactory $id_factory
     * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
     *
     * @return Loader
     */
    function to_es_bulk_update(
        array $config,
        string $index,
        IdFactory $id_factory,
        array $parameters = []
    ) : Loader {
        return ElasticsearchLoader::update($config, $index, $id_factory, $parameters);
    }

    /**
     * Transforms elasticsearch results into clear Flow Rows using ['hits']['hits'][x]['_source'].
     *
     * @return HitsIntoRowsTransformer
     */
    function es_hits_to_rows(DocumentDataSource $source = DocumentDataSource::source) : HitsIntoRowsTransformer
    {
        return new HitsIntoRowsTransformer($source);
    }

    /**
     * Extractor will automatically try to iterate over whole index using one of the two iteration methods:.
     *
     * - from/size
     * - search_after
     *
     * Search after is selected when you provide define sort parameters in query, otherwise it will fallback to from/size.
     *
     * @param array{
     *  hosts?: array<string>,
     *  connectionParams?: array<mixed>,
     *  retries?: int,
     *  sniffOnStart?: boolean,
     *  sslCert?: array<string>,
     *  sslKey?: array<string>,
     *  sslVerification?: boolean|string,
     *  elasticMetaHeader?: boolean,
     *  includePortInHostHeader?: boolean
     * } $config
     * @param array<mixed> $params - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
     * @param ?array<mixed> $pit_params - when used extractor will create point in time to stabilize search results. Point in time is automatically closed when last element is extracted. https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html
     */
    function from_es(array $config, array $params, ?array $pit_params = null) : ElasticsearchExtractor
    {
        return new ElasticsearchExtractor(
            $config,
            $params,
            $pit_params,
        );
    }
}

if (\class_exists('\Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor')) {
    /**
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
     * @param string $spreadsheet_id
     * @param string $sheet_name
     * @param bool $with_header
     * @param int $rows_per_page - how many rows per page to fetch from Google Sheets API
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     */
    function from_google_sheet(
        array|Sheets $auth_config,
        string $spreadsheet_id,
        string $sheet_name,
        bool $with_header = true,
        int $rows_per_page = 1000,
        array $options = [],
    ) : Extractor {
        if ($auth_config instanceof Sheets) {
            $sheets = $auth_config;
        } else {
            $client = new Client();
            $client->setScopes(Sheets::SPREADSHEETS_READONLY);
            $client->setAuthConfig($auth_config);
            $sheets = new Sheets($client);
        }

        return new GoogleSheetExtractor(
            $sheets,
            $spreadsheet_id,
            new Columns($sheet_name, 'A', 'Z'),
            $with_header,
            $rows_per_page,
            $options,
        );
    }

    /**
     * @param array{type: string, project_id: string, private_key_id: string, private_key: string, client_email: string, client_id: string, auth_uri: string, token_uri: string, auth_provider_x509_cert_url: string, client_x509_cert_url: string}|Sheets $auth_config
     * @param string $spreadsheet_id
     * @param string $sheet_name
     * @param string $start_range_column
     * @param string $end_range_column
     * @param bool $with_header
     * @param int $rows_per_page - how many rows per page to fetch from Google Sheets API
     * @param array{dateTimeRenderOption?: string, majorDimension?: string, valueRenderOption?: string} $options
     */
    function from_google_sheet_columns(
        array|Sheets $auth_config,
        string $spreadsheet_id,
        string $sheet_name,
        string $start_range_column,
        string $end_range_column,
        bool $with_header = true,
        int $rows_per_page = 1000,
        array $options = [],
    ) : Extractor {
        if ($auth_config instanceof Sheets) {
            $sheets = $auth_config;
        } else {
            $client = new Client();
            $client->setScopes(Sheets::SPREADSHEETS_READONLY);
            $client->setAuthConfig($auth_config);
            $sheets = new Sheets($client);
        }

        return new GoogleSheetExtractor(
            $sheets,
            $spreadsheet_id,
            new Columns($sheet_name, $start_range_column, $end_range_column),
            $with_header,
            $rows_per_page,
            $options,
        );
    }
}

if (\class_exists('\Flow\ETL\Adapter\JSON\JsonLoader')) {
    /**
     * @param array<Path|string>|Path|string $path - string is internally turned into LocalFile stream
     * @param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree
     *
     * @return Extractor
     */
    function from_json(
        string|Path|array $path,
        ?string $pointer = null,
    ) : Extractor {
        if (\is_array($path)) {
            $extractors = [];

            foreach ($path as $file) {
                $extractors[] = new JsonExtractor(
                    \is_string($file) ? Path::realpath($file) : $file,
                    $pointer,
                );
            }

            return from_all(...$extractors);
        }

        return new JsonExtractor(
            \is_string($path) ? Path::realpath($path) : $path,
            $pointer,
        );
    }

    /**
     * @param Path|string $path
     *
     * @return Loader
     */
    function to_json(string|Path $path) : Loader
    {
        return new JsonLoader(
            \is_string($path) ? Path::realpath($path) : $path,
        );
    }
}

if (\class_exists('\Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchExtractor')) {
    /**
     * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
     */
    function to_meilisearch_bulk_index(
        array $config,
        string $index,
    ) : Loader {
        return new MeilisearchLoader($config, $index);
    }

    /**
     * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
     */
    function to_meilisearch_bulk_update(
        array $config,
        string $index,
    ) : Loader {
        return MeilisearchLoader::update($config, $index);
    }

    /**
     * Transforms Meilisearch results into clear Flow Rows.
     */
    function meilisearch_hits_to_rows() : \Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\HitsIntoRowsTransformer
    {
        return new \Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\HitsIntoRowsTransformer();
    }

    /**
     * @param array{url: string, apiKey: string} $config
     * @param array{q: string, limit: ?int, offset: ?int, attributesToRetrieve: ?array<string>, sort: ?array<string>} $params
     */
    function from_meilisearch(array $config, array $params, string $index) : MeilisearchExtractor
    {
        return new MeilisearchExtractor($config, $params, $index);
    }
}

if (\class_exists('\Flow\ETL\Adapter\Parquet\ParquetExtractor')) {
    /**
     * @param array<Path>|Path|string $uri
     * @param array<string> $columns
     *
     * @return Extractor
     */
    function from_parquet(
        string|Path|array $uri,
        array $columns = [],
        Options $options = new Options(),
        ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
    ) : Extractor {
        if (\is_array($uri)) {
            $extractors = [];

            foreach ($uri as $filePath) {
                $extractors[] = new ParquetExtractor(
                    $filePath,
                    $options,
                    $byte_order,
                    $columns
                );
            }

            return from_all(...$extractors);
        }

        return new ParquetExtractor(
            \is_string($uri) ? Path::realpath($uri) : $uri,
            $options,
            $byte_order,
            $columns
        );
    }

    /**
     * @param Path|string $path
     * @param null|Schema $schema
     *
     * @return Loader
     */
    function to_parquet(
        string|Path $path,
        ?Options $options = null,
        Compressions $compressions = Compressions::SNAPPY,
        ?Schema $schema = null,
    ) : Loader {
        if ($options === null) {
            $options = Options::default();
        }

        return new ParquetLoader(
            \is_string($path) ? Path::realpath($path) : $path,
            $options,
            $compressions,
            $schema,
        );
    }
}

if (\class_exists('\Flow\ETL\Adapter\Text\TextExtractor')) {
    /**
     * @param array<Path|string>|Path|string $path
     *
     * @return Extractor
     */
    function from_text(
        string|Path|array $path,
    ) : Extractor {
        if (\is_array($path)) {
            $extractors = [];

            foreach ($path as $file_path) {
                $extractors[] = new TextExtractor(
                    \is_string($file_path) ? Path::realpath($file_path) : $file_path,
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new TextExtractor(
            \is_string($path) ? Path::realpath($path) : $path,
        );
    }

    /**
     * @param Path|string $path
     * @param string $new_line_separator
     *
     * @return Loader
     */
    function to_text(
        string|Path $path,
        string $new_line_separator = PHP_EOL
    ) : Loader {
        return new TextLoader(
            \is_string($path) ? Path::realpath($path) : $path,
            $new_line_separator
        );
    }
}

if (\class_exists('\Flow\ETL\Adapter\XML\XMLReaderExtractor')) {
    /**
     * @param array<Path|string>|Path|string $path
     * @param string $xml_node_path
     *
     * @return Extractor
     */
    function from_xml(
        string|Path|array $path,
        string $xml_node_path = ''
    ) : Extractor {
        if (\is_array($path)) {
            /** @var array<Extractor> $extractors */
            $extractors = [];

            foreach ($path as $next_path) {
                $extractors[] = new XMLReaderExtractor(
                    \is_string($next_path) ? Path::realpath($next_path) : $next_path,
                    $xml_node_path
                );
            }

            return from_all(...$extractors);
        }

        return new XMLReaderExtractor(
            \is_string($path) ? Path::realpath($path) : $path,
            $xml_node_path
        );
    }
}
