<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\DocumentationExample;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema as ParquetSchema;
use Generator;

use function count;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Parquet\empty_generator as parquet_empty_generator;
use function is_string;

/**
 * @param Path|string $path
 * @param array<string> $columns - list of columns to read from parquet file - @deprecated use `withColumns` method instead
 * @param Options $options - @deprecated use `withOptions` method instead
 * @param ByteOrder $byte_order - @deprecated use `withByteOrder` method instead
 * @param null|int $offset - @deprecated use `withOffset` method instead
 */
#[DocumentationDSL(module: Module::PARQUET, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'reading', example: 'parquet')]
function from_parquet(
    string|Path $path,
    array $columns = [],
    Options $options = new Options(),
    ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
    ?int $offset = null,
    ?ParquetEngine $engine = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): ParquetExtractor {
    $loader = (new ParquetExtractor(is_string($path) ? path_real($path) : $path, $filesystem))
        ->withOptions($options)
        ->withByteOrder($byte_order)
        ->withEngine($engine);

    if ($offset !== null) {
        $loader->withOffset($offset);
    }

    if (count($columns)) {
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
#[DocumentationExample(topic: 'writing', example: 'parquet')]
function to_parquet(
    string|Path $path,
    ?Options $options = null,
    Compressions $compressions = Compressions::SNAPPY,
    ?Schema $schema = null,
    ?ParquetEngine $engine = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): ParquetLoader {
    $loader = (new ParquetLoader(is_string($path) ? path_real($path) : $path, $filesystem))
        ->withCompressions($compressions)
        ->withEngine($engine);

    if ($options !== null) {
        $loader->withOptions($options);
    }

    if ($schema !== null) {
        $loader->withSchema($schema);
    }

    return $loader;
}

/**
 * @template T
 *
 * @param array<T> $data
 *
 * @return \Generator<T>
 */
#[DocumentationDSL(module: Module::PARQUET, type: DSLType::HELPER)]
function array_to_generator(array $data): Generator
{
    foreach ($data as $row) {
        yield $row;
    }
}

/**
 * @deprecated use Flow\Parquet\empty_generator() instead
 */
#[DocumentationDSL(module: Module::PARQUET, type: DSLType::HELPER)]
function empty_generator(): Generator
{
    return parquet_empty_generator();
}

#[DocumentationDSL(module: Module::PARQUET, type: DSLType::HELPER)]
function schema_to_parquet(Schema $schema): ParquetSchema
{
    return (new SchemaConverter())->toParquet($schema);
}

#[DocumentationDSL(module: Module::PARQUET, type: DSLType::HELPER)]
function schema_from_parquet(ParquetSchema $schema): Schema
{
    return (new SchemaConverter())->toFlow($schema);
}
