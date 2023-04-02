<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration\ElasticsearchPHP;

use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\DocumentDataSource;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Config;
use Flow\ETL\DSL\Elasticsearch;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class ElasticsearchExtractorTest extends TestCase
{
    public const INDEX_NAME = 'etl-test-index';

    protected function setUp() : void
    {
        parent::setUp();

        $this->elasticsearchContext->deleteIndex(self::INDEX_NAME);
        $this->elasticsearchContext->createIndex(self::INDEX_NAME);
    }

    protected function tearDown() : void
    {
        parent::tearDown();

        $this->elasticsearchContext->deleteIndex(self::INDEX_NAME);
    }

    public function test_empty_extraction() : void
    {
        $loader = Elasticsearch::bulk_index($this->elasticsearchContext->clientConfig(), 100, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

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
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body'  => [
                'query' => [
                    'match' => [
                        'title' => 'this_cant_be_matched',
                    ],
                ],
            ],
        ];

        $pitParams = [
            'index' => self::INDEX_NAME,
            'keep_alive' => '5m',
        ];

        $results = (new Flow())
            ->extract(Elasticsearch::search($this->elasticsearchContext->clientConfig(), $params, $pitParams))
            ->fetch();

        $this->assertCount(0, $results);
    }

    public function test_extraction_index_with_from_and_size() : void
    {
        $loader = Elasticsearch::bulk_index($this->elasticsearchContext->clientConfig(), 100, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    new Row\Entry\StringEntry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    new Row\Entry\StringEntry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) \random_int(0, 1))
                ),
                \range(1, 10_005)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body'  => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
                'fields' => [
                    'id',
                    'position',
                ],
                '_source' => false,
            ],
        ];

        $results = (new Flow())
            ->extract(Elasticsearch::search($this->elasticsearchContext->clientConfig(), $params))
            ->transform(Elasticsearch::hits_to_rows(DocumentDataSource::fields))
            ->fetch();

        $this->assertCount(10_000, $results);
        $this->assertArrayHasKey('id', $results->first()->toArray());
        $this->assertArrayHasKey('position', $results->first()->toArray());
        $this->assertArrayNotHasKey('active', $results->first()->toArray());
        $this->assertArrayNotHasKey('name', $results->first()->toArray());
    }

    public function test_extraction_index_with_search_after() : void
    {
        $loader = Elasticsearch::bulk_index($this->elasticsearchContext->clientConfig(), 100, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    new Row\Entry\StringEntry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    new Row\Entry\StringEntry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) \random_int(0, 1))
                ),
                \range(1, 10_005)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body'  => [
                'sort' => [
                    ['position' => ['order' => 'asc']],
                ],
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $results = (new Flow())
            ->extract(Elasticsearch::search($this->elasticsearchContext->clientConfig(), $params))
            ->fetch();

        $this->assertCount(10, $results);
    }

    public function test_extraction_index_with_search_after_with_point_in_time() : void
    {
        $loader = Elasticsearch::bulk_index($this->elasticsearchContext->clientConfig(), 100, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    new Row\Entry\StringEntry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    new Row\Entry\StringEntry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) \random_int(0, 1))
                ),
                \range(1, 10_005)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body'  => [
                'sort' => [
                    ['position' => ['order' => 'asc']],
                ],
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $pitParams = [
            'index' => self::INDEX_NAME,
            'keep_alive' => '5m',
        ];

        $results = (new Flow())
            ->extract(Elasticsearch::search($this->elasticsearchContext->clientConfig(), $params, $pitParams))
            ->fetch();

        $this->assertCount(10, $results);
    }

    public function test_extraction_whole_index_with_point_in_time() : void
    {
        $loader = Elasticsearch::bulk_index($this->elasticsearchContext->clientConfig(), 100, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    new Row\Entry\StringEntry('id', \sha1((string) $i)),
                    new Row\Entry\IntegerEntry('position', $i),
                    new Row\Entry\StringEntry('name', 'id_' . $i),
                    new Row\Entry\BooleanEntry('active', (bool) \random_int(0, 1))
                ),
                \range(1, 10_005)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body'  => [
                'sort' => [
                    ['position' => ['order' => 'asc']],
                ],
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $pitParams = [
            'index' => self::INDEX_NAME,
            'keep_alive' => '5m',
        ];

        $results = (new Flow())
            ->extract(Elasticsearch::search($this->elasticsearchContext->clientConfig(), $params, $pitParams))
            ->fetch();

        $this->assertCount(10, $results);
    }
}
