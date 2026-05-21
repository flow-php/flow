<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Elastic\Elasticsearch\Client as ElasticClient;
use Elastic\Elasticsearch\ClientBuilder as ElasticClientBuilder;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Rows;
use Flow\Types\Value\Json;
use Throwable;

use function class_exists;

final class ElasticsearchLoader implements Loader
{
    /** @phpstan-ignore-next-line */
    private Client|ElasticClient|null $client;

    private string $method;

    /**
     * @var array<mixed>
     */
    private array $parameters = [];

    /**
     * @param array{hosts?: array<string>, connectionParams?: array<mixed>, retries?: int, sniffOnStart?: bool, sslCert?: array<string>, sslKey?: array<string>, sslVerification?: (bool|string), elasticMetaHeader?: bool, includePortInHostHeader?: bool} $config
     */
    public function __construct(
        private readonly array $config,
        private readonly string $index,
        private readonly IdFactory $idFactory,
    ) {
        $this->client = null;
        $this->method = 'index';
    }

    /**
     * @param array{
     *  hosts?: array<string>,
     *  connectionParams?: array<mixed>,
     *  retries?: int,
     *  sniffOnStart?: bool,
     *  sslCert?: array<string>,
     *  sslKey?: array<string>,
     *  sslVerification?: bool|string,
     *  elasticMetaHeader?: bool,
     *  includePortInHostHeader?: bool
     * } $clientConfig
     */
    public static function update(array $clientConfig, string $index, IdFactory $idFactory): self
    {
        $loader = new self($clientConfig, $index, $idFactory);
        $loader->method = 'update';

        return $loader;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this);

        try {
            $factory = $this->idFactory;
            $parameters = $this->parameters;
            $parameters['body'] = [];

            /**
             * @var array<int, array{body:array<string, mixed>,id:string}> $dataCollection
             */
            $dataCollection = $rows->map(static fn(Row $row): Row => Row::create(
                $factory->create($row),
                new JsonEntry('body', Json::fromArray($row->toArray())),
            ))->toArray();

            foreach ($dataCollection as $data) {
                $parameters['body'][] = [
                    $this->method => [
                        '_id' => $data['id'],
                        '_index' => $this->index,
                    ],
                ];

                if ($this->method === 'update') {
                    $parameters['body'][] = ['doc' => $data['body']];
                } else {
                    $parameters['body'][] = $data['body'];
                }
            }

            /**
             * @phpstan-ignore-next-line
             */
            $this->client()->bulk($parameters);

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    /**
     * @param array<array-key, mixed> $parameters
     */
    public function withParameters(array $parameters): self
    {
        $this->parameters = $parameters;

        return $this;
    }

    /**
     * @phpstan-ignore-next-line
     */
    private function client(): Client|ElasticClient
    {
        if ($this->client === null) {
            if (class_exists("Elasticsearch\ClientBuilder")) {
                $this->client = ClientBuilder::fromConfig($this->config);
            } else {
                $this->client = ElasticClientBuilder::fromConfig($this->config);
            }
        }

        /**
         * @phpstan-ignore-next-line
         */
        return $this->client;
    }
}
