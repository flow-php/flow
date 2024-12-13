<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Row\Schema;
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\DocumentationExample, Attribute\Module, Attribute\Type};
use Flow\Filesystem\Path;

/**
 * @param Path|string $path - string is internally turned into stream
 * @param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree - @deprecate use withPointer method instead
 * @param ?Schema $schema - enforce schema on the extracted data - @deprecate use withSchema method instead
 */
#[DocumentationDSL(module: Module::JSON, type: Type::EXTRACTOR)]
#[DocumentationExample(topic: 'data_reading', example: 'json')]
function from_json(
    string|Path $path,
    ?string $pointer = null,
    ?Schema $schema = null,
) : JsonExtractor {
    $loader = new JsonExtractor(\is_string($path) ? Path::realpath($path) : $path);

    if ($pointer !== null) {
        $loader->withPointer($pointer);
    }

    if ($schema !== null) {
        $loader->withSchema($schema);
    }

    return $loader;
}

/**
 * @param Path|string $path
 * @param int $flags - PHP JSON Flags - @deprecate use withFlags method instead
 * @param string $date_time_format - format for DateTimeInterface::format() - @deprecate use withDateTimeFormat method instead
 * @param bool $put_rows_in_new_lines - if you want to put each row in a new line - @deprecate use withRowsInNewLines method instead
 *
 * @return JsonLoader
 */
#[DocumentationDSL(module: Module::JSON, type: Type::LOADER)]
function to_json(
    string|Path $path,
    int $flags = JSON_THROW_ON_ERROR,
    string $date_time_format = \DateTimeInterface::ATOM,
    bool $put_rows_in_new_lines = false,
) : JsonLoader {
    return (new JsonLoader(\is_string($path) ? Path::realpath($path) : $path))
        ->withFlags($flags)
        ->withDateTimeFormat($date_time_format)
        ->withRowsInNewLines($put_rows_in_new_lines);
}
