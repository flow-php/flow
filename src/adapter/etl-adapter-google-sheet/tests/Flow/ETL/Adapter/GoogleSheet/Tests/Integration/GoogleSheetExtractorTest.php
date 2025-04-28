<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\Tests\Integration;

use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\Adapter\GoogleSheet\{Columns, GoogleSheetExtractor};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Google\Client as GoogleClient;
use Google\Service\Sheets;
use GuzzleHttp\{Client, HandlerStack};
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\{Response};

final class GoogleSheetExtractorTest extends FlowTestCase
{
    public function test_extract_with_cut_extra_columns() : void
    {
        $client = new GoogleClient();
        $client->setHttpClient($this->createHttpClient());

        $extractor = new GoogleSheetExtractor(
            new Sheets($client),
            '1234567890',
            new Columns('Sheet', 'A', 'Z'),
        );

        $rows = $extractor->extract(flow_context(config()));

        foreach ($rows as $row) {
            self::assertNotNull($row);
        }
    }

    public function test_extract_without_cut_extra_columns() : void
    {
        $client = new GoogleClient();
        $client->setHttpClient($this->createHttpClient());

        $extractor = new GoogleSheetExtractor(
            new Sheets($client),
            '1234567890',
            new Columns('Sheet', 'A', 'Z'),
        );
        $extractor->withDropExtraColumns(false);

        $rows = $extractor->extract(flow_context(config()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row has more columns (4) than headers (3)');

        foreach ($rows as $row) {
            self::assertNotNull($row);
        }
    }

    private function createHttpClient() : Client
    {
        return new Client(
            [
                'handler' => HandlerStack::create(
                    new MockHandler(
                        [
                            new Response(
                                200,
                                ['Content-Type' => 'application/json'],
                                file_get_contents(__DIR__ . '/../Fixtures/extra-columns.json') ?: throw new \RuntimeException('Failed to read file')
                            ),
                        ]
                    )
                ),
            ]
        );
    }
}
