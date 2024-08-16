<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch;

use Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\{MeilisearchExtractor, MeilisearchLoader};
use Flow\ETL\Attribute\{DocumentationDSL, Module, Type};
use Flow\ETL\Loader;
use Psr\Http\Client\ClientInterface;

/**
 * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
 */
#[DocumentationDSL(module: Module::MEILI_SEARCH, type: Type::LOADER)]
function to_meilisearch_bulk_index(
    array $config,
    string $index,
) : Loader {
    return new MeilisearchLoader($config, $index);
}

/**
 * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
 */
#[DocumentationDSL(module: Module::MEILI_SEARCH, type: Type::LOADER)]
function to_meilisearch_bulk_update(
    array $config,
    string $index,
) : Loader {
    return MeilisearchLoader::update($config, $index);
}

/**
 * Transforms Meilisearch results into clear Flow Rows.
 */
#[DocumentationDSL(module: Module::MEILI_SEARCH, type: Type::HELPER)]
function meilisearch_hits_to_rows() : MeilisearchPHP\HitsIntoRowsTransformer
{
    return new MeilisearchPHP\HitsIntoRowsTransformer();
}

/**
 * @param array{url: string, apiKey: string} $config
 * @param array{q: string, limit: ?int, offset: ?int, attributesToRetrieve: ?array<string>, sort: ?array<string>} $params
 */
#[DocumentationDSL(module: Module::MEILI_SEARCH, type: Type::EXTRACTOR)]
function from_meilisearch(array $config, array $params, string $index) : MeilisearchExtractor
{
    return new MeilisearchExtractor($config, $params, $index);
}
