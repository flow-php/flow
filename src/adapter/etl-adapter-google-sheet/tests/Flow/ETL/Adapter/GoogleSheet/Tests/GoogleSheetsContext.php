<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests;

use Google\Client as GoogleClient;
use Google\Service\Sheets;
use GuzzleHttp\{Client as HttpClient, HandlerStack};
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;

final readonly class GoogleSheetsContext
{
    private GoogleClient $client;

    public function __construct()
    {
        $this->client = new GoogleClient();
    }

    public function sheets(string $fixtureFile) : Sheets
    {
        $this->client->setHttpClient($this->createHttpClient($fixtureFile));

        return new Sheets($this->client);
    }

    private function createHttpClient(string $fixtureFile) : HttpClient
    {
        return new HttpClient(
            [
                'handler' => HandlerStack::create(
                    new MockHandler(
                        [
                            new Response(
                                200,
                                ['Content-Type' => 'application/json'],
                                file_get_contents($fixtureFile) ?: throw new \RuntimeException('Failed to read file: ' . $fixtureFile)
                            ),
                        ]
                    )
                ),
            ]
        );
    }
}
