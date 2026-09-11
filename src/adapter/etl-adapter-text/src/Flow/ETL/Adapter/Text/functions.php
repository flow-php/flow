<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\path_real;
use function is_string;

/**
 * @param Path|string $path
 */
#[DocumentationDSL(module: Module::TEXT, type: Type::EXTRACTOR)]
function from_text(string|Path $path, Filesystem $filesystem = new NativeLocalFilesystem()): TextExtractor
{
    return new TextExtractor(is_string($path) ? path_real($path) : $path, $filesystem);
}

/**
 * @param Path|string $path
 * @param string $new_line_separator - default PHP_EOL - @deprecated use withNewLineSeparator method instead
 */
#[DocumentationDSL(module: Module::TEXT, type: Type::LOADER)]
function to_text(
    string|Path $path,
    string $new_line_separator = PHP_EOL,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): TextLoader {
    return (new TextLoader(is_string($path) ? path_real($path) : $path, $filesystem))->withNewLineSeparator(
        $new_line_separator,
    );
}
