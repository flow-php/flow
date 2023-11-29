<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Integration\MeilisearchPHP;

use function Flow\ETL\Adapter\Meilisearch\from_meilisearch;
use function Flow\ETL\Adapter\Meilisearch\meilisearch_hits_to_rows;
use function Flow\ETL\Adapter\Meilisearch\to_meilisearch_bulk_index;
use Flow\ETL\Adapter\Meilisearch\Tests\Context\MeilisearchContext;
use Flow\ETL\Config;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
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
                    new Row\Entry\StringEntry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    new Row\Entry\StringEntry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) \random_int(0, 1))
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

        $this->assertCount(0, $results);
    }

    public function test_extraction_index_with_from_and_size() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    new Row\Entry\StringEntry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    new Row\Entry\StringEntry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) \random_int(0, 1))
                ),
                // Default limit for Meilisearch is 1000 documents: https://www.meilisearch.com/docs/reference/api/settings#pagination
                \range(1, 999)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'q' => '',
            'offset' => 101,
            'attributesToRetrieve' => [
                'id',
                'position',
            ],
        ];

        $results = (new Flow())
            ->extract(from_meilisearch($this->meilisearchContext->clientConfig(), $params, self::INDEX_NAME))
            ->transform(meilisearch_hits_to_rows())
            ->fetch();

        $this->assertCount(999, $results);
        $this->assertArrayHasKey('id', $results->first()->toArray());
        $this->assertArrayHasKey('position', $results->first()->toArray());
        $this->assertArrayNotHasKey('active', $results->first()->toArray());
        $this->assertArrayNotHasKey('name', $results->first()->toArray());
    }

    public function test_extraction_index_with_sort() : void
    {
        $this->meilisearchContext->client()->index(self::INDEX_NAME)->updateSettings(['sortableAttributes' => ['position']]);

        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    new Row\Entry\StringEntry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    new Row\Entry\StringEntry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) \random_int(0, 1))
                ),
                // Default limit for Meilisearch is 1000 documents: https://www.meilisearch.com/docs/reference/api/settings#pagination
                \range(1, 999)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'q' => '',
            'sort' => ['position:desc'],
        ];

        $results = (new Flow())
            ->extract(from_meilisearch($this->meilisearchContext->clientConfig(), $params, self::INDEX_NAME))
            ->fetch();

        $this->assertCount(999, $results);
        $this->assertSame(999, $results->first()->toArray()['position']);
    }
}
