<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type, Extractor, Loader};
use Flow\Filesystem\Path;

/**
 * @param array<Path|string>|Path|string $path
 *
 * @return Extractor
 */
#[DocumentationDSL(module: Module::TEXT, type: Type::EXTRACTOR)]
function from_text(
    string|Path|array $path,
) : Extractor {
    if (\is_array($path)) {
        $extractors = [];

        foreach ($path as $file_path) {
            $extractors[] = new TextExtractor(
                \is_string($file_path) ? Path::realpath($file_path) : $file_path,
            );
        }

        return new Extractor\ChainExtractor(...$extractors);
    }

    return new TextExtractor(
        \is_string($path) ? Path::realpath($path) : $path,
    );
}

/**
 * @param Path|string $path
 * @param string $new_line_separator
 *
 * @return Loader
 */
#[DocumentationDSL(module: Module::TEXT, type: Type::LOADER)]
function to_text(
    string|Path $path,
    string $new_line_separator = PHP_EOL
) : Loader {
    return new TextLoader(
        \is_string($path) ? Path::realpath($path) : $path,
        $new_line_separator
    );
}
