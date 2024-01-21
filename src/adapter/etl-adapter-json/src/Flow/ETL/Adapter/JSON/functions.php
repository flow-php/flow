<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;

/**
 * @param array<Path|string>|Path|string $path - string is internally turned into stream
 * @param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree
 *
 * @return Extractor
 */
function from_json(
    string|Path|array $path,
    ?string $pointer = null,
    Schema|null $schema = null,
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
function to_json(string|Path $path) : Loader
{
    return new JsonLoader(
        \is_string($path) ? Path::realpath($path) : $path,
    );
}
