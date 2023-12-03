<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;

/**
 * @deprecated please use functions defined in Flow\ETL\DSL\functions.php
 */
class Json
{
    /**
     * @param array<Path|string>|Path|string $path - string is internally turned into stream
     * @param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree
     *
     * @return Extractor
     */
    public static function from(
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

            return new Extractor\ChainExtractor(...$extractors);
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
    public static function to(string|Path $path) : Loader
    {
        return new JsonLoader(
            \is_string($path) ? Path::realpath($path) : $path,
        );
    }
}
