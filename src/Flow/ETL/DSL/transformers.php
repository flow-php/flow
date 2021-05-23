<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\Cast\CastJsonToArray;
use Flow\ETL\Transformer\Cast\CastToDateTime;
use Flow\ETL\Transformer\Cast\CastToJson;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\EntryNameCaseConverterTransformer;
use Flow\ETL\Transformer\Filter\Filter\Callback;
use Flow\ETL\Transformer\FilterRowsTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;

function filter(string $entryName, callable $filter) : Transformer
{
    return new FilterRowsTransformer(new Callback(fn (Row $row) : bool => $filter($row->valueOf($entryName))));
}

function keepColumns(string ...$names) : Transformer
{
    return new KeepEntriesTransformer(...$names);
}

function columnNameConvert(string $style) : Transformer
{
    if (!\class_exists('Jawira\CaseConverter\Convert')) {
        throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
    }

    return new EntryNameCaseConverterTransformer($style);
}

function castTo(string $toType, array $columns, $format = 'c', ?string $tz = null, ?string $toTz = null) : Transformer
{
    switch (\strtolower($toType)) {
        case 'datetime' :
            return new CastTransformer(CastToDateTime::nullable($columns, $format, $tz, $toTz));
        case 'json' :
            return new CastTransformer(CastToJson::nullable($columns));
        case 'array':
            return new CastTransformer(CastJsonToArray::nullable($columns));

        default:
            throw new InvalidArgumentException("Unrecognized type {$toType}");
    }
}

function expand(string $arrayColumn) : Transformer
{
    return new Transformer\ArrayExpandTransformer($arrayColumn);
}

function unpack(string $arrayColumn) : Transformer
{
    return new Transformer\ArrayUnpackTransformer($arrayColumn);
}
