<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Factory\NativeEntryFactory;

final class ElasticsearchExtractor implements Extractor
{
    /**
     * @phpstan-ignore-next-line
     *
     * @psalm-suppress UndefinedClass
     */
    private \Elasticsearch\Client|\Elastic\Elasticsearch\Client|null $client;

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
     * @param array<mixed> $params - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
     * @param ?array<mixed> $pointInTimeParams - https://www.elastic.co/guide/en/elasticsearch/reference/master/point-in-time-api.html
     */
    public function __construct(
        private readonly array $config,
        private readonly array $params,
        private readonly ?array $pointInTimeParams = null,
        private readonly Row\EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
        $this->client = null;
    }

    /**
     * @psalm-suppress PossiblyUndefinedMethod
     * @psalm-suppress MixedArgument
     */
    public function extract(FlowContext $context) : \Generator
    {
        /**
         * @psalm-suppress UndefinedClass
         * @psalm-suppress PossiblyInvalidArgument
         */
        $pit = \is_array($this->pointInTimeParams)
            /**
             * @phpstan-ignore-next-line
             */
            ? new PointInTime($this->client()->openPointInTime($this->pointInTimeParams))
            : null;

        $params = ($pit)
            ? new SearchParams(\array_merge($this->params, ['pit' => ['id' => $pit->id()]]))
            : new SearchParams($this->params);

        /**
         * @psalm-suppress UndefinedClass
         * @psalm-suppress PossiblyInvalidArgument
         *
         * @phpstan-ignore-next-line
         */
        $results = new SearchResults($this->client()->search($params->asArray()));

        if ($results->size() === 0) {
            $this->closePointInTime($pit);

            return;
        }

        yield $results->toRows($this->entryFactory);

        // Go with search_after pagination
        if ($params->hasSort()) {
            $lastHitSort = $results->lastHitSort();

            while (true) {
                $nextPageParams = $params->setBody('search_after', $lastHitSort);

                /**
                 * @psalm-suppress UndefinedClass
                 * @psalm-suppress PossiblyInvalidArgument
                 *
                 * @phpstan-ignore-next-line
                 */
                $nextResults = new SearchResults($this->client()->search($nextPageParams->asArray()));
                $lastHitSort = $nextResults->lastHitSort();

                if (!$nextResults->size()) {
                    break;
                }

                yield $nextResults->toRows($this->entryFactory);
            }
        } else {
            $fetched = $results->size();
            // go with from/size pagination which is not recommended but will work for most of the small indexes.
            for ($page = 1; $page <= $results->pages(); $page++) {
                $nextPageParams = $params
                    ->set('from', $page * $results->size())
                    ->set('size', $results->size());

                /**
                 * @psalm-suppress MixedOperand
                 */
                if ($nextPageParams->asArray()['from'] >= $results->total()) {
                    break;
                }

                /**
                 * @psalm-suppress MixedOperand
                 */
                if ($nextPageParams->asArray()['from'] + $nextPageParams->asArray()['size'] > $results->total()) {
                    $nextPageParams = $nextPageParams->set('size', $results->total() - $fetched);
                }

                /**
                 * @psalm-suppress UndefinedClass
                 * @psalm-suppress PossiblyInvalidArgument
                 *
                 * @phpstan-ignore-next-line
                 */
                $nextResults = new SearchResults($this->client()->search($nextPageParams->asArray()));

                $fetched += $nextResults->size();

                yield $nextResults->toRows($this->entryFactory);
            }
        }

        $this->closePointInTime($pit);
    }

    /**
     * @psalm-suppress UndefinedClass
     * @psalm-suppress MixedAssignment
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedInferredReturnType
     *
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

        return $this->client;
    }

    /**
     * @psalm-suppress ImpureMethodCall
     *
     * @throws \Elastic\Elasticsearch\Exception\ClientResponseException
     * @throws \Elastic\Elasticsearch\Exception\ServerResponseException
     */
    private function closePointInTime(?PointInTime $pit) : void
    {
        if ($pit) {
            /**
             * @psalm-suppress UndefinedClass
             *
             * @phpstan-ignore-next-line
             */
            $this->client()->closePointInTime(['body' => ['id' => $pit->id()]]);
        }
    }
}
