<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Row\Schema;
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type, Loader};
use Flow\Filesystem\Path;

/**
 * @param Path|string $path - string is internally turned into stream
 * @param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree
 */
#[DocumentationDSL(module: Module::JSON, type: Type::EXTRACTOR)]
function from_json(
    string|Path $path,
    ?string $pointer = null,
    ?Schema $schema = null,
) : JsonExtractor {
    return new JsonExtractor(
        \is_string($path) ? Path::realpath($path) : $path,
        $pointer,
        $schema
    );
}

/**
 * @param Path|string $path
 *
 * @return Loader
 */
#[DocumentationDSL(module: Module::JSON, type: Type::LOADER)]
function to_json(
    string|Path $path,
    int $flags = JSON_THROW_ON_ERROR,
    string $date_time_format = \DateTimeInterface::ATOM,
    bool $put_rows_in_new_lines = false
) : Loader {
    return new JsonLoader(\is_string($path) ? Path::realpath($path) : $path, $flags, $date_time_format, $put_rows_in_new_lines);
}
