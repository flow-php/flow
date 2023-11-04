<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Meilisearch\Client;

final class MeilisearchExtractor implements Extractor
{
    private Client|null $client = null;

    /**
     * @param array{url: string, apiKey: string} $config
     * @param array{q: string, limit: ?int, offset: ?int, attributesToRetrieve: ?array<string>, sort: ?array<string>} $params
     */
    public function __construct(
        private readonly array $config,
        private readonly array $params,
        private readonly string $index,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $params = new SearchParams($this->params);

        $results = new SearchResults($this->client()->index($this->index)->search($params->getQuery(), $params->asArray()));

        if ($results->total() === 0) {
            return;
        }

        yield $results->toRows($context->entryFactory());

        $fetched = $results->size();

        // go with from/size pagination which is not recommended but will work for most of the small indexes.
        for ($page = 1; $page <= $results->pages(); $page++) {
            $nextPageParams = $params
                ->set('offset', $page * $results->size())
                ->set('limit', $results->size());

            if ($nextPageParams->asArray()['offset'] >= $results->total()) {
                break;
            }

            if ($nextPageParams->asArray()['offset'] + $nextPageParams->asArray()['limit'] > $results->total()) {
                $nextPageParams = $nextPageParams->set('limit', $results->total() - $fetched);
            }

            $nextResults = new SearchResults($this->client()->index($this->index)->search($nextPageParams->getQuery(), $nextPageParams->asArray()));

            $fetched += $nextResults->size();

            $signal = yield $nextResults->toRows($context->entryFactory());

            if ($signal === Extractor\Signal::STOP) {
                return;
            }
        }
    }

    private function client() : Client
    {
        if ($this->client === null) {
            $this->client = new Client($this->config['url'], $this->config['apiKey']);
        }

        return $this->client;
    }
}
