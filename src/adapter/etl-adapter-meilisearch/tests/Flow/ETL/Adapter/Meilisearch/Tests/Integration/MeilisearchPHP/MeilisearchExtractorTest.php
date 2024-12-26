<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Integration\MeilisearchPHP;

use function Flow\ETL\Adapter\Meilisearch\{from_meilisearch, meilisearch_hits_to_rows, to_meilisearch_bulk_index};
use function Flow\ETL\DSL\{generate_random_int, string_entry};
use Flow\ETL\Adapter\Meilisearch\Tests\Context\MeilisearchContext;
use Flow\ETL\{Config, Flow, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class MeilisearchExtractorTest extends TestCase
{
    public const INDEX_NAME = 'etl-test-index';

    private MeilisearchContext $meilisearchContext;

    protected function setUp() : void
    {
        $this->meilisearchContext = new MeilisearchContext(\getenv('MEILISEARCH_URL'), \getenv('MEILISEARCH_API_KEY'));
        $this->meilisearchContext->createIndex(self::INDEX_NAME);
    }

    protected function tearDown() : void
    {
        parent::tearDown();

        $this->meilisearchContext->deleteIndex(self::INDEX_NAME);
    }

    public function test_empty_extraction() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    string_entry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) generate_random_int(0, 1))
                ),
                \range(1, 100)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'q' => 'title=this_cant_be_matched',
        ];

        $results = (new Flow())
            ->extract(from_meilisearch($this->meilisearchContext->clientConfig(), $params, self::INDEX_NAME))
            ->fetch();

        self::assertCount(0, $results);
    }

    public function test_extraction_index_with_from_and_size() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    string_entry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) generate_random_int(0, 1))
                ),
                // Default limit for Meilisearch is 1000 documents: https://www.meilisearch.com/docs/reference/api/settings#pagination
                \range(1, 100)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'q' => '',
            'offset' => 51,
            'attributesToRetrieve' => [
                'id',
                'position',
            ],
        ];

        $results = (new Flow())
            ->extract(from_meilisearch($this->meilisearchContext->clientConfig(), $params, self::INDEX_NAME))
            ->transform(meilisearch_hits_to_rows())
            ->fetch();

        self::assertCount(49, $results);
        self::assertArrayHasKey('id', $results->first()->toArray());
        self::assertArrayHasKey('position', $results->first()->toArray());
        self::assertArrayNotHasKey('active', $results->first()->toArray());
        self::assertArrayNotHasKey('name', $results->first()->toArray());
    }

    public function test_extraction_index_with_sort() : void
    {
        $this->meilisearchContext->client()->index(self::INDEX_NAME)->updateSettings(['sortableAttributes' => ['position']]);

        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    string_entry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) generate_random_int(0, 1))
                ),
                // Default limit for Meilisearch is 1000 documents: https://www.meilisearch.com/docs/reference/api/settings#pagination
                \range(1, 100)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'q' => '',
            'sort' => ['position:desc'],
        ];

        $results = (new Flow())
            ->extract(from_meilisearch($this->meilisearchContext->clientConfig(), $params, self::INDEX_NAME))
            ->fetch();

        self::assertCount(100, $results);
        self::assertSame(100, $results->first()->toArray()['position']);
    }
}
