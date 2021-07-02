<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ArrayKeysCaseConverterTransformer;
use Flow\ETL\Transformer\Cast\CastEntries;
use Flow\ETL\Transformer\Cast\CastJsonToArray;
use Flow\ETL\Transformer\Cast\CastToDate;
use Flow\ETL\Transformer\Cast\CastToDateTime;
use Flow\ETL\Transformer\Cast\CastToInteger;
use Flow\ETL\Transformer\Cast\CastToJson;
use Flow\ETL\Transformer\Cast\CastToString;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\EntryNameCaseConverterTransformer;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\Filter\Filter\EntryEqualsTo;
use Flow\ETL\Transformer\Filter\Filter\EntryExists;
use Flow\ETL\Transformer\Filter\Filter\EntryNotNull;
use Flow\ETL\Transformer\Filter\Filter\EntryNumber;
use Flow\ETL\Transformer\Filter\Filter\Opposite;
use Flow\ETL\Transformer\FilterRowsTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\Rename\ArrayKeyRename;
use Flow\ETL\Transformer\Rename\EntryRename;
use Flow\ETL\Transformer\RenameEntriesTransformer;
use Laminas\Hydrator\ReflectionHydrator;

/**
 * @param string $column
 * @param callable(mixed) : bool $filter
 */
function filter(string $column, callable $filter) : Transformer
{
    return new FilterRowsTransformer(new Callback(fn (Row $row) : bool => $filter($row->valueOf($column))));
}

/**
 * @param string $column
 * @param mixed $value
 */
function filter_equals(string $column, $value) : Transformer
{
    return new FilterRowsTransformer(new EntryEqualsTo($column, $value));
}

/**
 * @param string $column
 * @param mixed $value
 */
function filter_not_equals(string $column, $value) : Transformer
{
    return new FilterRowsTransformer(new Opposite(new EntryEqualsTo($column, $value)));
}

function filter_exists(string $column) : Transformer
{
    return new FilterRowsTransformer(new EntryExists($column));
}

function filter_not_exists(string $column) : Transformer
{
    return new FilterRowsTransformer(new Opposite(new EntryExists($column)));
}

function filter_null(string $column) : Transformer
{
    return new FilterRowsTransformer(new Opposite(new EntryNotNull($column)));
}

function filter_not_null(string $column) : Transformer
{
    return new FilterRowsTransformer(new EntryNotNull($column));
}

function filter_number(string $column) : Transformer
{
    return new FilterRowsTransformer(new EntryNumber($column));
}

function filter_not_number(string $column) : Transformer
{
    return new FilterRowsTransformer(new Opposite(new EntryNumber($column)));
}

function keep(string ...$columns) : Transformer
{
    return new KeepEntriesTransformer(...$columns);
}

function remove(string ...$columns) : Transformer
{
    return new Transformer\RemoveEntriesTransformer(...$columns);
}

function rename(string $from, string $to) : Transformer
{
    return new RenameEntriesTransformer(new EntryRename($from, $to));
}

function clone_column(string $from, string $to) : Transformer
{
    return new Transformer\CloneEntryTransformer($from, $to);
}

function convert_name(string $style) : Transformer
{
    if (!\class_exists('Jawira\CaseConverter\Convert')) {
        throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
    }

    return new EntryNameCaseConverterTransformer($style);
}

/**
 * @param string[] $columns
 * @param string $format
 * @param ?string $timezone
 * @param ?string $to_timezone
 */
function to_datetime(array $columns, string $format = 'c', ?string $timezone = null, ?string $to_timezone = null) : Transformer
{
    return new CastTransformer(CastToDateTime::nullable($columns, $format, $timezone, $to_timezone));
}

/**
 * @param string[] $columns
 * @param string $format
 * @param ?string $timezone
 * @param ?string $to_timezone
 */
function to_datetime_cast(array $columns, string $format = 'c', ?string $timezone = null, ?string $to_timezone = null) : CastEntries
{
    return CastToDateTime::nullable($columns, $format, $timezone, $to_timezone);
}

function to_date(string ...$columns) : Transformer
{
    return new CastTransformer(CastToDate::nullable($columns));
}

function to_date_cast(string ...$columns) : CastEntries
{
    return CastToDate::nullable($columns);
}

function to_integer(string ...$columns) : Transformer
{
    return new CastTransformer(CastToInteger::nullable($columns));
}

function to_integer_cast(string ...$columns) : CastEntries
{
    return CastToInteger::nullable($columns);
}

function to_string(string ...$columns) : Transformer
{
    return new CastTransformer(CastToString::nullable($columns));
}

function to_string_cast(string ...$columns) : CastEntries
{
    return CastToString::nullable($columns);
}

function to_json(string ...$columns) : Transformer
{
    return new CastTransformer(CastToJson::nullable($columns));
}

function to_json_cast(string ...$columns) : CastEntries
{
    return CastToJson::nullable($columns);
}

function to_array_from_json(string ...$columns) : Transformer
{
    return new CastTransformer(CastJsonToArray::nullable($columns));
}

function to_array_from_json_cast(string ...$columns) : CastEntries
{
    return CastJsonToArray::nullable($columns);
}

function to_null_from_null_string(string ...$columns) : Transformer
{
    return new Transformer\NullStringIntoNullEntryTransformer(...$columns);
}

function to_array_from_object(string $column) : Transformer
{
    if (!\class_exists('Laminas\Hydrator\ReflectionHydrator')) {
        throw new RuntimeException("Laminas\Hydrator\ReflectionHydrator class not found, please install it using 'composer require laminas/laminas-hydrator'");
    }

    return new Transformer\ObjectToArrayTransformer(new ReflectionHydrator(), $column);
}

function expand(string $array_column, string $expanded_name = 'column') : Transformer
{
    return new Transformer\ArrayExpandTransformer($array_column, $expanded_name);
}

/**
 * @param string $array_column
 * @param string $column_prefix
 * @param string[] $skip_keys
 */
function unpack(string $array_column, string $column_prefix = '', array $skip_keys = []) : Transformer
{
    return new Transformer\ArrayUnpackTransformer($array_column, $skip_keys, $column_prefix);
}

/**
 * @param string[] $string_columns
 * @param string $glue
 * @param string $column_name
 */
function concat(array $string_columns, string $glue = '', string $column_name = 'column') : Transformer
{
    return new Transformer\StringConcatTransformer($string_columns, $glue, $column_name);
}

function array_get(string $array_name, string $path, string $column_name = 'column') : Transformer
{
    return new Transformer\ArrayDotGetTransformer($array_name, $path, $column_name);
}

function array_sort(string $array_name, int $sort_flag = \SORT_REGULAR) : Transformer
{
    return new Transformer\ArraySortTransformer($array_name, $sort_flag);
}

function array_reverse(string $array_name) : Transformer
{
    return new Transformer\ArrayReverseTransformer($array_name);
}

/**
 * @param string[] $array_names
 * @param string $column_name
 */
function array_merge(array $array_names, string $column_name = 'column') : Transformer
{
    return new Transformer\ArrayMergeTransformer($array_names, $column_name);
}

function array_rename_keys(string $array_column, string $path, string $new_name) : Transformer
{
    return new Transformer\ArrayDotRenameTransformer(new ArrayKeyRename($array_column, $path, $new_name));
}

function array_convert_keys(string $array_column, string $style) : Transformer
{
    if (!\class_exists('Jawira\CaseConverter\Convert')) {
        throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
    }

    return new ArrayKeysCaseConverterTransformer($array_column, $style);
}

/**
 * @param string $object_name
 * @param string $method
 * @param string $column_name
 * @param array<mixed> $parameters
 */
function object_method(string $object_name, string $method, string $column_name = 'column', array $parameters = []) : Transformer
{
    return new Transformer\ObjectMethodTransformer($object_name, $method, $column_name, $parameters);
}

function add_string(string $name, string $value) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\StringEntry($name, $value));
}

function add_integer(string $name, int $value) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\IntegerEntry($name, $value));
}

function add_boolean(string $name, bool $value) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\BooleanEntry($name, $value));
}

function add_float(string $name, float $value) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\FloatEntry($name, $value));
}

function add_date(string $name, string $value) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\DateEntry($name, new \DateTimeImmutable($value)));
}

function add_datetime(string $name, string $value, string $format = \DateTimeImmutable::ATOM) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\DateTimeEntry($name, new \DateTimeImmutable($value), $format));
}

/**
 * @param string $name
 * @param array<mixed> $data
 */
function add_array(string $name, array $data) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\ArrayEntry($name, $data));
}

/**
 * @param string $name
 * @param array<mixed> $data
 */
function add_json(string $name, array $data) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\JsonEntry($name, $data));
}

/**
 * @param string $name
 * @param array<mixed> $data
 */
function add_json_object(string $name, array $data) : Transformer
{
    return new Transformer\StaticEntryTransformer(Row\Entry\JsonEntry::object($name, $data));
}

function add_object(string $name, object $data) : Transformer
{
    return new Transformer\StaticEntryTransformer(new Row\Entry\ObjectEntry($name, $data));
}

function chain(Transformer ...$transformers) : Transformer
{
    return new Transformer\ChainTransformer(...$transformers);
}

function transform_if(Transformer\Condition\RowCondition $condition, Transformer $transformer) : Transformer
{
    return new Transformer\ConditionalTransformer($condition, $transformer);
}
