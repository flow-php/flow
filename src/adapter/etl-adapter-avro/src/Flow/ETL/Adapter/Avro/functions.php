<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Adapter\Avro;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Adapter\Avro\FlixTech\AvroLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Row\Schema;

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

function to_avro(Path|string $path, ?Schema $schema = null) : AvroLoader
{
    return new AvroLoader(
        \is_string($path) ? Path::realpath($path) : $path,
        $schema
    );
}
