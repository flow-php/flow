<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use DateTimeInterface;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonLinesExtractor;
use Flow\ETL\Adapter\JSON\JsonSchema\ReferenceResolver;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\DocumentationExample;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type;
use Flow\ETL\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;

use function Flow\Filesystem\DSL\path_real;
use function is_string;

/**
 * @param Path|string $path - string is internally turned into stream
 * @param ?string $pointer - if you want to iterate only results of a subtree, use a pointer, read more at https://github.com/halaxa/json-machine#parsing-a-subtree - @deprecate use withPointer method instead
 * @param null|Schema $schema - enforce schema on the extracted data - @deprecate use withSchema method instead
 */
#[DocumentationDSL(module: Module::JSON, type: Type::EXTRACTOR)]
#[DocumentationExample(topic: 'data_frame', example: 'data_reading', option: 'json')]
function from_json(
    string|Path $path,
    ?string $pointer = null,
    ?Schema $schema = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): JsonExtractor {
    $loader = new JsonExtractor(is_string($path) ? path_real($path) : $path, $filesystem);

    if ($pointer !== null) {
        $loader->withPointer($pointer);
    }

    if ($schema !== null) {
        $loader->withSchema($schema);
    }

    return $loader;
}

/**
 * Used to read from a JSON lines https://jsonlines.org/ formatted file.
 *
 * @param Path|string $path - string is internally turned into stream
 */
#[DocumentationDSL(module: Module::JSON, type: Type::EXTRACTOR)]
#[DocumentationExample(topic: 'data_frame', example: 'data_reading', option: 'jsonl')]
function from_json_lines(string|Path $path, Filesystem $filesystem = new NativeLocalFilesystem()): JsonLinesExtractor
{
    return new JsonLinesExtractor(is_string($path) ? path_real($path) : $path, $filesystem);
}

/**
 * @param Path|string $path
 * @param int $flags - PHP JSON Flags - @deprecate use withFlags method instead
 * @param string $date_time_format - format for DateTimeInterface::format() - @deprecate use withDateTimeFormat method instead
 * @param bool $put_rows_in_new_lines - if you want to put each row in a new line - @deprecate use withRowsInNewLines method instead
 *
 * @return JsonLoader
 */
#[DocumentationDSL(module: Module::JSON, type: Type::LOADER)]
function to_json(
    string|Path $path,
    int $flags = JSON_THROW_ON_ERROR,
    string $date_time_format = DateTimeInterface::ATOM,
    bool $put_rows_in_new_lines = false,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): JsonLoader {
    return (new JsonLoader(is_string($path) ? path_real($path) : $path, $filesystem))
        ->withFlags($flags)
        ->withDateTimeFormat($date_time_format)
        ->withRowsInNewLines($put_rows_in_new_lines);
}

/**
 * Used to write to a JSON lines https://jsonlines.org/ formatted file.
 *
 * @param Path|string $path
 *
 * @return JsonLinesLoader
 */
#[DocumentationDSL(module: Module::JSON, type: Type::LOADER)]
function to_json_lines(string|Path $path, Filesystem $filesystem = new NativeLocalFilesystem()): JsonLinesLoader
{
    return new JsonLinesLoader(is_string($path) ? path_real($path) : $path, $filesystem);
}

/**
 * Convert a JSON Schema (https://json-schema.org) document into a Flow Schema.
 *
 * @param array<string, mixed>|Path|string $json_schema - decoded document, raw JSON document or a path to a schema file
 * @param null|ClientInterface $client - PSR-18 http client, required to resolve remote http(s) references
 * @param null|RequestFactoryInterface $request_factory - PSR-17 request factory, required to resolve remote http(s) references
 * @param Filesystem $filesystem - filesystem used to read local schema references
 */
#[DocumentationDSL(module: Module::JSON, type: Type::HELPER)]
function schema_from_json_schema(
    string|array|Path $json_schema,
    ?ClientInterface $client = null,
    ?RequestFactoryInterface $request_factory = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): Schema {
    return (new SchemaConverter(new ReferenceResolver($client, $request_factory, $filesystem)))->toFlow($json_schema);
}

/**
 * Convert a Flow Schema into a JSON Schema (https://json-schema.org, draft 2020-12) document.
 *
 * @return array<string, mixed>
 */
#[DocumentationDSL(module: Module::JSON, type: Type::HELPER)]
function schema_to_json_schema(Schema $schema): array
{
    return (new SchemaConverter())->toJsonSchema($schema);
}
