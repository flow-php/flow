<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\Expression\Literal;
use Flow\ETL\Row\StructureReference;

function col(string $entry, string ...$entries) : Reference
{
    if (\count($entries)) {
        return new StructureReference($entry, ...$entries);
    }

    return new EntryReference($entry);
}

function entry(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

/**
 * Alias for entry function.
 */
function ref(string $entry) : EntryReference
{
    return entry($entry);
}

function struct(string ...$entries) : StructureReference
{
    if (!\count($entries)) {
        throw new InvalidArgumentException('struct (StructureReference) require at least one entry');
    }

    $entry = \array_shift($entries);

    return new StructureReference($entry, ...$entries);
}

function lit(mixed $value) : Expression
{
    return new Literal($value);
}

function when(Expression $ref, Expression $then, Expression $else = null) : Expression
{
    return new Expression\When($ref, $then, $else);
}

function array_get(Expression $ref, string $path) : Expression
{
    return new Expression\ArrayGet($ref, $path);
}

function array_exists(Expression $ref, string $path) : Expression
{
    return new Expression\ArrayExists($ref, $path);
}

function now(\DateTimeZone $time_zone = new \DateTimeZone('UTC')) : Expression
{
    return new Expression\Now($time_zone);
}

function to_date_time(Expression $ref, string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : Expression
{
    return new Expression\ToDateTime($ref, $format, $timeZone);
}

function date_time_format(Expression $ref, string $format) : Expression
{
    return new Expression\DateTimeFormat($ref, $format);
}

/**
 * @param non-empty-string $separator
 */
function split(Expression $ref, string $separator, int $limit = PHP_INT_MAX) : Expression
{
    return new Expression\Split($ref, $separator, $limit);
}
