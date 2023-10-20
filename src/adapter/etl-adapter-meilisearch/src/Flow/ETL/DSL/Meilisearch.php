<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\HitsIntoRowsTransformer;
use Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchExtractor;
use Flow\ETL\Adapter\Meilisearch\MeilisearchPHP\MeilisearchLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Transformer;

class Meilisearch
{
    /**
     * @param array{url: string, apiKey: string} $config
     */
    final public static function bulk_index(
        array $config,
        string $index,
    ) : Loader {
        return new MeilisearchLoader($config, $index);
    }

    /**
     * @param array{url: string, apiKey: string} $config
     */
    final public static function bulk_update(
        array $config,
        string $index,
    ) : Loader {
        return MeilisearchLoader::update($config, $index);
    }

    /**
     * Transforms Meilisearch results into clear Flow Rows.
     */
    final public static function hits_to_rows() : Transformer
    {
        return new HitsIntoRowsTransformer();
    }

    /**
     * @param array{url: string, apiKey: string} $config
     * @param array{q: string, limit: ?int, offset: ?int, attributesToRetrieve: ?array<string>, sort: ?array<string>} $params
     */
    final public static function search(array $config, array $params, string $index) : Extractor
    {
        return new MeilisearchExtractor($config, $params, $index);
    }
}
