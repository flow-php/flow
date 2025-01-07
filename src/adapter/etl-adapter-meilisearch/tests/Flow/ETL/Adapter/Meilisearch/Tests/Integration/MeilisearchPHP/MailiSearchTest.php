<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Integration\MeilisearchPHP;

use function Flow\ETL\Adapter\Meilisearch\{from_meilisearch, meilisearch_hits_to_rows, to_meilisearch_bulk_index};
use function Flow\ETL\DSL\{from_array, string_entry};
use Flow\ETL\Adapter\Meilisearch\Tests\Context\MeilisearchContext;
use Flow\ETL\Adapter\Meilisearch\Tests\Double\Spy\HttpClientSpy;
use Flow\ETL\{Flow, Row, Rows, Tests\FlowTestCase};

final class MailiSearchTest extends FlowTestCase
{
    private const DESTINATION_INDEX = 'etl-test-destination-index';

    private const SOURCE_INDEX = 'etl-test-source-index';

    private MeilisearchContext $meilisearchContext;

    protected function setUp() : void
    {
        $this->meilisearchContext = new MeilisearchContext(\getenv('MEILISEARCH_URL'), \getenv('MEILISEARCH_API_KEY'));
        $this->meilisearchContext->deleteIndex(self::SOURCE_INDEX);
        $this->meilisearchContext->createIndex(self::SOURCE_INDEX);
        $this->meilisearchContext->deleteIndex(self::DESTINATION_INDEX);
        $this->meilisearchContext->createIndex(self::DESTINATION_INDEX);
    }

    protected function tearDown() : void
    {
        $this->meilisearchContext->deleteIndex(self::SOURCE_INDEX);
        $this->meilisearchContext->deleteIndex(self::DESTINATION_INDEX);
    }

    public function test_batch_size_when_its_not_explicitly_set() : void
    {
        (new Flow())
            ->read(from_array([
                ['id' => 1, 'text' => 'lorem ipsum'],
                ['id' => 2, 'text' => 'lorem ipsum'],
                ['id' => 3, 'text' => 'lorem ipsum'],
                ['id' => 4, 'text' => 'lorem ipsum'],
                ['id' => 5, 'text' => 'lorem ipsum'],
                ['id' => 6, 'text' => 'lorem ipsum'],
            ]))
            ->write(
                to_meilisearch_bulk_index(
                    \array_merge(
                        $this->meilisearchContext->clientConfig(),
                        ['httpClient' => $httpClient = new HttpClientSpy()]
                    ),
                    'test',
                )
            )
            ->run();

        self::assertCount(
            2, // second request is to check if the first one was processed
            $httpClient->requests
        );
    }

    public function test_loading_and_extraction_with_limit_and_transformation() : void
    {
        $this->meilisearchContext->loadRows(
            new Rows(
                ...\array_map(
                    static fn (int $i) : Row => Row::create(
                        string_entry('id', \sha1((string) $i)),
                        new Row\Entry\IntegerEntry('position', $i),
                        string_entry('name', 'id_' . $i),
                        new Row\Entry\BooleanEntry('active', false)
                    ),
                    \range(1, 101)
                ),
            ),
            self::SOURCE_INDEX
        );

        $params = [
            'q' => '',
            'limit' => $limit = 100,
        ];

        $results = (new Flow())
            ->extract(from_meilisearch($this->meilisearchContext->clientConfig(), $params, self::SOURCE_INDEX))
            ->rows(meilisearch_hits_to_rows())
            ->limit($limit)
            ->load(
                to_meilisearch_bulk_index(
                    $this->meilisearchContext->clientConfig(),
                    self::DESTINATION_INDEX
                )
            )
            ->fetch();

        self::assertCount($limit, $results);
        self::assertSame(
            \array_map(
                static fn (int $i) : array => [
                    'id' => \sha1((string) $i),
                    'position' => $i,
                    'name' => 'id_' . $i,
                    'active' => false,
                ],
                \range(1, $limit)
            ),
            $results->toArray()
        );
    }
}
