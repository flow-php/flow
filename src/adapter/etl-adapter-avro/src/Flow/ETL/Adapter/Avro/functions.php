<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Adapter\Avro;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Adapter\Avro\FlixTech\{AvroExtractor, AvroLoader};
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type, Extractor, Schema};
use Flow\Filesystem\Path;

#[DocumentationDSL(module: Module::AVRO, type: Type::EXTRACTOR)]
function from_avro(Path|string|array $path) : Extractor
{
    if (\is_array($path)) {
        /** @var array<Extractor> $extractors */
        $extractors = [];

        foreach ($path as $next_path) {
            $extractors[] = new AvroExtractor(
                \is_string($next_path) ? Path::realpath($next_path) : $next_path,
            );
        }

        return from_all(...$extractors);
    }

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
