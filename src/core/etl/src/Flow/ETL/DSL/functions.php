<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

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
use Flow\ETL\Partition;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;

function col(string $entry) : Reference
{
    return new EntryReference($entry);
}

function entry(string $entry) : Reference
{
    return new EntryReference($entry);
}

/**
 * Alias for entry function.
 */
function ref(string $entry) : Reference
{
    return entry($entry);
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
