<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Row\Schema;
use Flow\ETL\{Attribute\DSL, Attribute\Module, Attribute\Type, Extractor, Loader};
use Flow\Filesystem\Path;

/**
 * @param array<Path|string>|Path|string $path - string is internally turned into stream
 * @param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree
 */
#[DSL(module: Module::JSON, type: Type::EXTRACTOR)]
function from_json(
    string|Path|array $path,
    ?string $pointer = null,
    ?Schema $schema = null,
) : Extractor {
    if (\is_array($path)) {
        $extractors = [];

        foreach ($path as $file) {
            $extractors[] = new JsonExtractor(
                \is_string($file) ? Path::realpath($file) : $file,
                $pointer,
                $schema
            );
        }

        return from_all(...$extractors);
    }

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
#[DSL(module: Module::JSON, type: Type::LOADER)]
function to_json(
    string|Path $path,
    int $flags = JSON_THROW_ON_ERROR,
    string $date_time_format = \DateTimeInterface::ATOM,
    bool $put_rows_in_new_lines = false
) : Loader {
    return new JsonLoader(\is_string($path) ? Path::realpath($path) : $path, $flags, $date_time_format, $put_rows_in_new_lines);
}
