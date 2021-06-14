<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Cast\CastEntry;
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
use Flow\ETL\Transformer\RenameEntries\EntryRename;
use Flow\ETL\Transformer\RenameEntriesTransformer;
use Laminas\Hydrator\ReflectionHydrator;

function filter(string $column, callable $filter) : Transformer
{
    return new FilterRowsTransformer(new Callback(fn (Row $row) : bool => $filter($row->valueOf($column))));
}

/**
 * @param string $column
 * @param mixed $value
 */
function filterEquals(string $column, $value) : Transformer
{
    return new FilterRowsTransformer(new EntryEqualsTo($column, $value));
}

/**
 * @param string $column
 * @param mixed $value
 */
function filterNotEquals(string $column, $value) : Transformer
{
    return new FilterRowsTransformer(new Opposite(new EntryEqualsTo($column, $value)));
}

function filterExists(string $column) : Transformer
{
    return new FilterRowsTransformer(new EntryExists($column));
}

function filterNotExists(string $column) : Transformer
{
    return new FilterRowsTransformer(new Opposite(new EntryExists($column)));
}

function filterNull(string $column) : Transformer
{
    return new FilterRowsTransformer(new Opposite(new EntryNotNull($column)));
}

function filterNotNull(string $column) : Transformer
{
    return new FilterRowsTransformer(new EntryNotNull($column));
}

function filterNumber(string $column) : Transformer
{
    return new FilterRowsTransformer(new EntryNumber($column));
}

function filterNotNumber(string $column) : Transformer
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

function cloneColumn(string $from, string $to) : Transformer
{
    return new Transformer\CloneEntryTransformer($from, $to);
}

function convertName(string $style) : Transformer
{
    if (!\class_exists('Jawira\CaseConverter\Convert')) {
        throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
    }

    return new EntryNameCaseConverterTransformer($style);
}

function toDateTime(array $columns, $format = 'c', ?string $tz = null, ?string $toTz = null) : Transformer
{
    return new CastTransformer(CastToDateTime::nullable($columns, $format, $tz, $toTz));
}

function toDateTimeCast(array $columns, $format = 'c', ?string $tz = null, ?string $toTz = null) : CastEntry
{
    return CastToDateTime::nullable($columns, $format, $tz, $toTz);
}

function toDate(string ...$columns) : Transformer
{
    return new CastTransformer(CastToDate::nullable($columns));
}

function toDateCast(string ...$columns) : CastEntry
{
    return CastToDate::nullable($columns);
}

function toInteger(string ...$columns) : Transformer
{
    return new CastTransformer(CastToInteger::nullable($columns));
}

function toIntegerCast(string ...$columns) : CastEntry
{
    return CastToInteger::nullable($columns);
}

function toString(string ...$columns) : Transformer
{
    return new CastTransformer(CastToString::nullable($columns));
}

function toStringCast(string ...$columns) : CastEntry
{
    return CastToString::nullable($columns);
}

function toJson(string ...$columns) : Transformer
{
    return new CastTransformer(CastToJson::nullable($columns));
}

function toJsonCast(string ...$columns) : CastEntry
{
    return CastToJson::nullable($columns);
}

function toArrayFromJson(string ...$columns) : Transformer
{
    return new CastTransformer(CastJsonToArray::nullable($columns));
}

function toArrayFromJsonCast(string ...$columns) : CastEntry
{
    return CastJsonToArray::nullable($columns);
}

function toNullFromNullString(string ...$columns) : Transformer
{
    return new Transformer\NullStringIntoNullEntryTransformer(...$columns);
}

function toArrayFromObject(string $column) : Transformer
{
    if (!\class_exists('Laminas\Hydrator\ReflectionHydrator')) {
        throw new RuntimeException("Laminas\Hydrator\ReflectionHydrator class not found, please install it using 'composer require laminas/laminas-hydrator'");
    }

    return new Transformer\ObjectToArrayTransformer(new ReflectionHydrator(), $column);
}

function expand(string $arrayColumn) : Transformer
{
    return new Transformer\ArrayExpandTransformer($arrayColumn);
}

function unpack(string $arrayColumn) : Transformer
{
    return new Transformer\ArrayUnpackTransformer($arrayColumn);
}
