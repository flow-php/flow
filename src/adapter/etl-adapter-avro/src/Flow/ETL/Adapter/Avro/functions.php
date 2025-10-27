<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Adapter\Avro;

use Flow\ETL\Adapter\Avro\FlixTech\{AvroExtractor, AvroLoader};
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type, Schema};
use Flow\Filesystem\Path;

#[DocumentationDSL(module: Module::AVRO, type: Type::EXTRACTOR)]
function from_avro(Path|string $path) : AvroExtractor
{
    return new AvroExtractor(
        \is_string($path) ? Path::realpath($path) : $path
    );
}

#[DocumentationDSL(module: Module::AVRO, type: Type::LOADER)]
function to_avro(Path|string $path, ?Schema $schema = null) : AvroLoader
{
    return new AvroLoader(
        \is_string($path) ? Path::realpath($path) : $path,
        $schema
    );
}
