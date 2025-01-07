<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

use Flow\ETL\{FlowContext, Loader, Row, Rows};
use Meilisearch\Client;
use Psr\Http\Client\ClientInterface;

final class MeilisearchLoader implements Loader
{
    private ?Client $client = null;

    /**
     * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
     */
    public function __construct(
        private array $config,
        private readonly string $index,
    ) {
    }

    /**
     * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
     */
    public static function update(array $config, string $index) : self
    {
        return new self($config, $index);
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $dataCollection = $rows->map(fn (Row $row) : Row => Row::create(
            ...$row->map(
                fn (Row\Entry $entry) : Row\Entry => $entry
            )->entries()
        ))->toArray();

        $promise = $this->client()->index($this->index)->updateDocuments($dataCollection);
        $this->client()->waitForTask($promise['taskUid']);
    }

    private function client() : Client
    {
        if ($this->client === null) {
            $this->client = new Client($this->config['url'], $this->config['apiKey'], $this->config['httpClient'] ?? null);
        }

        return $this->client;
    }
}
