<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Adapter\Avro;

use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Adapter\Avro\FlixTech\AvroLoader;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\path_real;

#[DocumentationDSL(module: Module::AVRO, type: Type::EXTRACTOR)]
function from_avro(Path|string $path): AvroExtractor
{
    return new AvroExtractor(\is_string($path) ? path_real($path) : $path);
}

#[DocumentationDSL(module: Module::AVRO, type: Type::LOADER)]
function to_avro(Path|string $path, ?Schema $schema = null): AvroLoader
{
    return new AvroLoader(\is_string($path) ? path_real($path) : $path, $schema);
}
