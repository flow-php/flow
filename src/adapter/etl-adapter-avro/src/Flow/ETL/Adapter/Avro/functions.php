<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Adapter\Avro;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Adapter\Avro\FlixTech\AvroLoader;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\path_real;
use function is_string;

#[DocumentationDSL(module: Module::AVRO, type: Type::EXTRACTOR)]
function from_avro(Path|string $path, Filesystem $filesystem = new NativeLocalFilesystem()): AvroExtractor
{
    return new AvroExtractor(is_string($path) ? path_real($path) : $path, $filesystem);
}

#[DocumentationDSL(module: Module::AVRO, type: Type::LOADER)]
function to_avro(
    Path|string $path,
    ?Schema $schema = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): AvroLoader {
    return new AvroLoader(is_string($path) ? path_real($path) : $path, $schema, $filesystem);
}
