<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Row\Schema;
use Flow\ETL\{Attribute\DocumentationDSL,
    Attribute\DocumentationExample,
    Attribute\Module,
    Attribute\Type as DSLType
};
use Flow\Filesystem\Path;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\{ByteOrder, Options};

/**
 * @param Path|string $path
 * @param array<string> $columns - list of columns to read from parquet file - @deprecated use `withColumns` method instead
 * @param Options $options - @deprecated use `withOptions` method instead
 * @param ByteOrder $byte_order - @deprecated use `withByteOrder` method instead
 * @param null|int $offset - @deprecated use `withOffset` method instead
 */
#[DocumentationDSL(module: Module::PARQUET, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_reading', example: 'parquet')]
function from_parquet(
    string|Path $path,
    array $columns = [],
    Options $options = new Options(),
    ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
    ?int $offset = null,
) : ParquetExtractor {
    $loader = (new ParquetExtractor(\is_string($path) ? Path::realpath($path) : $path))
        ->withOptions($options)
        ->withByteOrder($byte_order);

    if ($offset !== null) {
        $loader->withOffset($offset);
    }

    if (\count($columns)) {
        $loader->withColumns($columns);
    }

    return $loader;
}

/**
 * @param Path|string $path
 * @param null|Options $options - @deprecated use `withOptions` method instead
 * @param Compressions $compressions - @deprecated use `withCompressions` method instead
 * @param null|Schema $schema - @deprecated use `withSchema` method instead
 */
#[DocumentationDSL(module: Module::PARQUET, type: DSLType::LOADER)]
#[DocumentationExample(topic: 'data_writing', example: 'parquet')]
function to_parquet(
    string|Path $path,
    ?Options $options = null,
    Compressions $compressions = Compressions::SNAPPY,
    ?Schema $schema = null,
) : ParquetLoader {
    $loader = (new ParquetLoader(\is_string($path) ? Path::realpath($path) : $path))
        ->withCompressions($compressions);

    if ($options !== null) {
        $loader->withOptions($options);
    }

    if ($schema !== null) {
        $loader->withSchema($schema);
    }

    return $loader;
}
