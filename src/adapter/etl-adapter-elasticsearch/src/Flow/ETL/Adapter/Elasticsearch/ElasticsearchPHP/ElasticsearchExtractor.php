<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Elastic\Elasticsearch\Client as ElasticClient;
use Elastic\Elasticsearch\ClientBuilder as ElasticClientBuilder;
use Elastic\Elasticsearch\Exception\ClientResponseException;
use Elastic\Elasticsearch\Exception\ServerResponseException;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

use function class_exists;
use function is_array;

final class ElasticsearchExtractor implements Extractor
{
    // @mago-ignore analysis:non-existent-class-like
    /** @phpstan-ignore-next-line */
    private Client|ElasticClient|null $client;

    /**
     * @var null|array<array-key, mixed>
     */
    private ?array $pointInTimeParams = null;

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
     * } $config
     * @param array<mixed> $parameters - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
     */
    public function __construct(
        private readonly array $config,
        private readonly array $parameters,
    ) {
        $this->client = null;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $pit = is_array($this->pointInTimeParams)
            // @mago-ignore analysis:invalid-method-access
            // @mago-ignore analysis:possibly-invalid-argument
            // @mago-ignore analysis:mixed-argument
            /** @phpstan-ignore-next-line */
            ? new PointInTime($this->client()->openPointInTime($this->pointInTimeParams))
            : null;

        $params = $pit
            ? (new SearchParams($this->parameters))
                ->setBody('pit', ['id' => $pit->id()])
                ->remove('index')
            : new SearchParams($this->parameters);

        // @mago-ignore analysis:invalid-method-access
        // @mago-ignore analysis:possibly-invalid-argument
        // @mago-ignore analysis:mixed-argument
        /** @phpstan-ignore-next-line */
        $results = new SearchResults($this->client()->search($params->asArray()));

        if ($results->size() === 0) {
            $this->closePointInTime($pit);

            return;
        }

        $rows = $results->toRows($context->entryFactory());

        $signal = yield $rows;

        if ($signal === Signal::STOP) {
            $this->closePointInTime($pit);

            return;
        }

        // Go with search_after pagination
        if ($params->hasSort()) {
            $lastHitSort = $results->lastHitSort();

            while (true) {
                $nextPageParams = $params->setBody('search_after', $lastHitSort);

                // @mago-ignore analysis:invalid-method-access
                // @mago-ignore analysis:possibly-invalid-argument
                // @mago-ignore analysis:mixed-argument
                /** @phpstan-ignore-next-line */
                $nextResults = new SearchResults($this->client()->search($nextPageParams->asArray()));
                $lastHitSort = $nextResults->lastHitSort();

                if (!$nextResults->size()) {
                    break;
                }

                $rows = $nextResults->toRows($context->entryFactory());

                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    $this->closePointInTime($pit);

                    return;
                }
            }
        } else {
            $fetched = $results->size();

            // go with from/size pagination which is not recommended but will work for most of the small indexes.
            for ($page = 1; $page <= $results->pages(); $page++) {
                $nextPageParams = $params->set('from', $page * $results->size())->set('size', $results->size());

                /** @var int $from */
                $from = $nextPageParams->asArray()['from'];

                if ($from >= $results->total()) {
                    break;
                }

                /** @var int $size */
                $size = $nextPageParams->asArray()['size'];

                if (($from + $size) > $results->total()) {
                    $nextPageParams = $nextPageParams->set('size', $results->total() - $fetched);
                }

                // @mago-ignore analysis:invalid-method-access
                // @mago-ignore analysis:possibly-invalid-argument
                // @mago-ignore analysis:mixed-argument
                /** @phpstan-ignore-next-line */
                $nextResults = new SearchResults($this->client()->search($nextPageParams->asArray()));

                $fetched += $nextResults->size();

                $rows = $nextResults->toRows($context->entryFactory());

                $signal = yield $rows;

                if ($signal === Signal::STOP) {
                    $this->closePointInTime($pit);

                    return;
                }
            }
        }

        $this->closePointInTime($pit);
    }

    /**
     * @param array<mixed> $pointInTimeParams - https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html
     */
    public function withPointInTime(array $pointInTimeParams): self
    {
        $this->pointInTimeParams = $pointInTimeParams;

        return $this;
    }

    // @mago-ignore analysis:non-existent-class-like
    /** @phpstan-ignore-next-line */
    private function client(): Client|ElasticClient
    {
        if ($this->client === null) {
            if (class_exists("Elasticsearch\ClientBuilder")) {
                // @mago-ignore analysis:non-existent-method
                // @mago-ignore analysis:mixed-property-type-coercion
                $this->client = ClientBuilder::fromConfig($this->config);
            } else {
                $this->client = ElasticClientBuilder::fromConfig($this->config);
            }
        }

        // @mago-ignore analysis:nullable-return-statement
        // @mago-ignore analysis:invalid-return-statement
        /** @phpstan-ignore-next-line */
        return $this->client;
    }

    /**
     * @throws ClientResponseException
     * @throws ServerResponseException
     */
    private function closePointInTime(?PointInTime $pit): void
    {
        if ($pit) {
            // @mago-ignore analysis:invalid-method-access
            /** @phpstan-ignore-next-line */
            $this->client()->closePointInTime(['body' => ['id' => $pit->id()]]);
        }
    }
}
