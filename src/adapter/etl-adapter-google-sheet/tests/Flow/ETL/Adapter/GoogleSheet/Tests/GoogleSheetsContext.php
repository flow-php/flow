<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests;

use Google\Client as GoogleClient;
use Google\Service\Sheets;

final readonly class GoogleSheetsContext
{
    private GoogleClient $client;

    public function __construct(private HttpClientContext $httpClientContext = new HttpClientContext())
    {
        $this->client = new GoogleClient();
    }

    public function sheets() : Sheets
    {
        $this->client->setHttpClient($this->httpClientContext->createHttpClient());

        return new Sheets($this->client);
    }
}
