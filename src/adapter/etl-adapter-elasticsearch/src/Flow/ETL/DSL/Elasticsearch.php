<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\DocumentDataSource;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchExtractor;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchLoader;
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\HitsIntoRowsTransformer;
use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Transformer;

class Elasticsearch
{
    /**
     * https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.
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
     * @param int $chunk_size
     * @param string $index
     * @param IdFactory $id_factory
     * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
     *
     * @return Loader
     */
    final public static function bulk_index(
        array $config,
        int $chunk_size,
        string $index,
        IdFactory $id_factory,
        array $parameters = []
    ) : Loader {
        return new ElasticsearchLoader($config, $chunk_size, $index, $id_factory, $parameters);
    }

    /**
     *  https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html.
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
     * @param int $chunk_size
     * @param string $index
     * @param IdFactory $id_factory
     * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
     *
     * @return Loader
     */
    final public static function bulk_update(
        array $config,
        int $chunk_size,
        string $index,
        IdFactory $id_factory,
        array $parameters = []
    ) : Loader {
        return ElasticsearchLoader::update($config, $chunk_size, $index, $id_factory, $parameters);
    }

    /**
     * Transforms elasticsearch results into clear Flow Rows using ['hits']['hits'][x]['_source'].
     *
     * @return Transformer
     */
    final public static function hits_to_rows(DocumentDataSource $source = DocumentDataSource::source) : Transformer
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
     * @param array<mixed> $params - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
     * @param ?array<mixed> $pit_params - when used extractor will create point in time to stabilize search results. Point in time is automatically closed when last element is extracted. https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html
     */
    final public static function search(array $config, array $params, ?array $pit_params = null) : Extractor
    {
        return new ElasticsearchExtractor(
            $config,
            $params,
            $pit_params,
        );
    }
}
