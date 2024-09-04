<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type, Loader};
use Flow\Filesystem\Path;

#[DocumentationDSL(module: Module::TEXT, type: Type::EXTRACTOR)]
function from_text(
    string|Path $path,
) : TextExtractor {
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
