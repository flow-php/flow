<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch;

use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\{DocumentDataSource, ElasticsearchExtractor, ElasticsearchLoader, HitsIntoRowsTransformer};
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\{EntryIdFactory, HashIdFactory};
use Flow\ETL\Attribute\{DocumentationDSL, DocumentationExample, Module, Type};

/**
 * https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.
 *
 * In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array{
 *  hosts?: array<string>,
 *  connectionParams?: array<mixed>,
 *  retries?: int,
 *  sniffOnStart?: boolean,
 *  sslCert?: array<string>,
 *  sslKey?: array<string>,
 *  sslVerification?: boolean|string,
 *  elasticMetaHeader?: boolean,
 *  includePortInHostHeader?: boolean
 * } $config
 * @param string $index
 * @param IdFactory $id_factory
 * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead
 */
#[DocumentationDSL(module: Module::ELASTIC_SEARCH, type: Type::LOADER)]
#[DocumentationExample(topic: 'data_writing', example: 'elasticsearch')]
function to_es_bulk_index(
    array $config,
    string $index,
    IdFactory $id_factory,
    array $parameters = [],
) : ElasticsearchLoader {
    return (new ElasticsearchLoader($config, $index, $id_factory))->withParameters($parameters);
}

#[DocumentationDSL(module: Module::ELASTIC_SEARCH, type: Type::HELPER)]
#[DocumentationExample(topic: 'data_writing', example: 'elasticsearch')]
function entry_id_factory(string $entry_name) : IdFactory
{
    return new EntryIdFactory($entry_name);
}

#[DocumentationDSL(module: Module::ELASTIC_SEARCH, type: Type::HELPER)]
function hash_id_factory(string ...$entry_names) : IdFactory
{
    return new HashIdFactory(...$entry_names);
}

/**
 *  https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.
 *
 * In order to control the size of the single request, use DataFrame::chunkSize() method just before calling DataFrame::load().
 *
 * @param array{
 *  hosts?: array<string>,
 *  connectionParams?: array<mixed>,
 *  retries?: int,
 *  sniffOnStart?: boolean,
 *  sslCert?: array<string>,
 *  sslKey?: array<string>,
 *  sslVerification?: boolean|string,
 *  elasticMetaHeader?: boolean,
 *  includePortInHostHeader?: boolean
 * } $config
 * @param string $index
 * @param IdFactory $id_factory
 * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html - @deprecated use withParameters method instead
 */
#[DocumentationDSL(module: Module::ELASTIC_SEARCH, type: Type::LOADER)]
function to_es_bulk_update(
    array $config,
    string $index,
    IdFactory $id_factory,
    array $parameters = [],
) : ElasticsearchLoader {
    return ElasticsearchLoader::update($config, $index, $id_factory)->withParameters($parameters);
}

/**
 * Transforms elasticsearch results into clear Flow Rows using ['hits']['hits'][x]['_source'].
 *
 * @return HitsIntoRowsTransformer
 */
#[DocumentationDSL(module: Module::ELASTIC_SEARCH, type: Type::HELPER)]
#[DocumentationExample(topic: 'data_writing', example: 'elasticsearch')]
function es_hits_to_rows(DocumentDataSource $source = DocumentDataSource::source) : HitsIntoRowsTransformer
{
    return new HitsIntoRowsTransformer($source);
}

/**
 * Extractor will automatically try to iterate over whole index using one of the two iteration methods:.
 *
 * - from/size
 * - search_after
 *
 * Search after is selected when you provide define sort parameters in query, otherwise it will fallback to from/size.
 *
 * @param array{
 *  hosts?: array<string>,
 *  connectionParams?: array<mixed>,
 *  retries?: int,
 *  sniffOnStart?: boolean,
 *  sslCert?: array<string>,
 *  sslKey?: array<string>,
 *  sslVerification?: boolean|string,
 *  elasticMetaHeader?: boolean,
 *  includePortInHostHeader?: boolean
 * } $config
 * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
 * @param ?array<mixed> $pit_params - when used extractor will create point in time to stabilize search results. Point in time is automatically closed when last element is extracted. https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html - @deprecated use withPointInTime method instead
 */
#[DocumentationDSL(module: Module::ELASTIC_SEARCH, type: Type::EXTRACTOR)]
#[DocumentationExample(topic: 'data_reading', example: 'elasticsearch')]
function from_es(array $config, array $parameters, ?array $pit_params = null) : ElasticsearchExtractor
{
    $extractor = new ElasticsearchExtractor($config, $parameters);

    if ($pit_params) {
        $extractor->withPointInTime($pit_params);
    }

    return $extractor;
}
