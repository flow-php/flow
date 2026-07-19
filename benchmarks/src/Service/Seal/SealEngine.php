<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Seal;

use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema as SealSchema;
use Elastic\Elasticsearch\ClientBuilder;
use Flow\Benchmarks\Service\Services;
use RuntimeException;
use Throwable;

final class SealEngine
{
    public static function open(SealSchema $schema): EngineInterface
    {
        $url = Services::elasticsearchUrl();

        try {
            $client = ClientBuilder::create()->setHosts([$url])->build();
            $client->info();
        } catch (Throwable $e) {
            throw new RuntimeException(
                'Cannot connect to Elasticsearch at '
                    . $url
                    . ' (seal benchmarks require Elasticsearch 8.x). Start the docker compose services (docker compose up -d elasticsearch) or override ELASTICSEARCH_URL. Original error: '
                    . $e->getMessage(),
                previous: $e,
            );
        }

        return new Engine(new ElasticsearchAdapter($client), $schema);
    }

    public static function recreateIndex(EngineInterface $engine, string $index): void
    {
        if ($engine->existIndex($index)) {
            $engine->dropIndex($index, ['return_slow_promise_result' => true])?->wait();
        }

        $engine->createIndex($index, ['return_slow_promise_result' => true])?->wait();
    }

    public static function dropIndex(EngineInterface $engine, string $index): void
    {
        if ($engine->existIndex($index)) {
            $engine->dropIndex($index, ['return_slow_promise_result' => true])?->wait();
        }
    }
}
