<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Flow\ETL\{Extractor, FlowContext};

final class ElasticsearchExtractor implements Extractor
{
    /**
     * @phpstan-ignore-next-line
     */
    private \Elasticsearch\Client|\Elastic\Elasticsearch\Client|null $client;

    private ?array $pointInTimeParams = null;

    /**
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
     */
    public function __construct(
        private readonly array $config,
        private readonly array $parameters,
    ) {
        $this->client = null;
    }

    public function extract(FlowContext $context) : \Generator
    {
        $pit = \is_array($this->pointInTimeParams)
            /**
             * @phpstan-ignore-next-line
             */
            ? new PointInTime($this->client()->openPointInTime($this->pointInTimeParams))
            : null;

        $params = ($pit)
            ? (new SearchParams($this->parameters))->setBody('pit', ['id' => $pit->id()])->remove('index')
            : new SearchParams($this->parameters);

        /**
         * @phpstan-ignore-next-line
         */
        $results = new SearchResults($this->client()->search($params->asArray()));

        if ($results->size() === 0) {
            $this->closePointInTime($pit);

            return;
        }

        yield $results->toRows($context->entryFactory());

        // Go with search_after pagination
        if ($params->hasSort()) {
            $lastHitSort = $results->lastHitSort();

            while (true) {
                $nextPageParams = $params->setBody('search_after', $lastHitSort);

                /**
                 * @phpstan-ignore-next-line
                 */
                $nextResults = new SearchResults($this->client()->search($nextPageParams->asArray()));
                $lastHitSort = $nextResults->lastHitSort();

                if (!$nextResults->size()) {
                    break;
                }

                yield $nextResults->toRows($context->entryFactory());
            }
        } else {
            $fetched = $results->size();

            // go with from/size pagination which is not recommended but will work for most of the small indexes.
            for ($page = 1; $page <= $results->pages(); $page++) {
                $nextPageParams = $params
                    ->set('from', $page * $results->size())
                    ->set('size', $results->size());

                if ($nextPageParams->asArray()['from'] >= $results->total()) {
                    break;
                }

                /**
                 * @phpstan-ignore-next-line
                 */
                if ($nextPageParams->asArray()['from'] + $nextPageParams->asArray()['size'] > $results->total()) {
                    $nextPageParams = $nextPageParams->set('size', $results->total() - $fetched);
                }

                /**
                 * @phpstan-ignore-next-line
                 */
                $nextResults = new SearchResults($this->client()->search($nextPageParams->asArray()));

                $fetched += $nextResults->size();

                $signal = yield $nextResults->toRows($context->entryFactory());

                if ($signal === Extractor\Signal::STOP) {
                    return;
                }
            }
        }

        $this->closePointInTime($pit);
    }

    /**
     * @param array<mixed> $pointInTimeParams - https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html
     */
    public function withPointInTime(array $pointInTimeParams) : self
    {
        $this->pointInTimeParams = $pointInTimeParams;

        return $this;
    }

    /**
     * @phpstan-ignore-next-line
     */
    private function client() : \Elasticsearch\Client|\Elastic\Elasticsearch\Client
    {
        if ($this->client === null) {
            if (\class_exists("Elasticsearch\ClientBuilder")) {
                $this->client = \Elasticsearch\ClientBuilder::fromConfig($this->config);
            } else {
                $this->client = \Elastic\Elasticsearch\ClientBuilder::fromConfig($this->config);
            }
        }

        /**
         * @phpstan-ignore-next-line
         */
        return $this->client;
    }

    /**
     * @throws \Elastic\Elasticsearch\Exception\ClientResponseException
     * @throws \Elastic\Elasticsearch\Exception\ServerResponseException
     */
    private function closePointInTime(?PointInTime $pit) : void
    {
        if ($pit) {
            /**
             * @phpstan-ignore-next-line
             */
            $this->client()->closePointInTime(['body' => ['id' => $pit->id()]]);
        }
    }
}
