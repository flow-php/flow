<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration\ElasticsearchPHP;

use function Flow\ETL\Adapter\Elasticsearch\{es_hits_to_rows, from_es, to_es_bulk_index};
use function Flow\ETL\DSL\{bool_entry, df, generate_random_int, int_entry, string_entry};
use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\DocumentDataSource;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\{Config, Flow, FlowContext, Row, Rows};

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
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    int_entry('position', $i),
                    string_entry('name', 'id_' . $i),
                    bool_entry('active', (bool) generate_random_int(0, 1))
                ),
                \range(1, 100)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body' => [
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

        $results = df()
            ->read(from_es($this->elasticsearchContext->clientConfig(), $params, $pitParams))
            ->fetch();

        self::assertCount(0, $results);
    }

    public function test_extraction_index_with_from_and_size() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    int_entry('position', $i),
                    string_entry('name', 'id_' . $i),
                    bool_entry('active', (bool) generate_random_int(0, 1))
                ),
                \range(1, 2000)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body' => [
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
            ->extract(from_es($this->elasticsearchContext->clientConfig(), $params))
            ->transform(es_hits_to_rows(DocumentDataSource::fields))
            ->fetch();

        self::assertCount(2000, $results);
        self::assertArrayHasKey('id', $results->first()->toArray());
        self::assertArrayHasKey('position', $results->first()->toArray());
        self::assertArrayNotHasKey('active', $results->first()->toArray());
        self::assertArrayNotHasKey('name', $results->first()->toArray());
    }

    public function test_extraction_index_with_search_after() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    int_entry('position', $i),
                    string_entry('name', 'id_' . $i),
                    bool_entry('active', (bool) generate_random_int(0, 1))
                ),
                \range(1, 2005)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body' => [
                'sort' => [
                    ['position' => ['order' => 'asc']],
                ],
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $results = (new Flow())
            ->extract(from_es($this->elasticsearchContext->clientConfig(), $params))
            ->fetch();

        self::assertCount(3, $results);
    }

    public function test_extraction_index_with_search_after_with_point_in_time() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    int_entry('position', $i),
                    string_entry('name', 'id_' . $i),
                    bool_entry('active', (bool) generate_random_int(0, 1))
                ),
                \range(1, 2005)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body' => [
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
            ->extract(from_es($this->elasticsearchContext->clientConfig(), $params, $pitParams))
            ->fetch();

        self::assertCount(3, $results);
    }

    public function test_extraction_whole_index_with_point_in_time() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            ...\array_map(
                static fn (int $i) : Row => Row::create(
                    string_entry('id', \sha1((string) $i)),
                    int_entry('position', $i),
                    string_entry('name', 'id_' . $i),
                    bool_entry('active', (bool) generate_random_int(0, 1))
                ),
                \range(1, 2005)
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'size' => 1001,
            'body' => [
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
            ->extract(from_es($this->elasticsearchContext->clientConfig(), $params, $pitParams))
            ->fetch();

        self::assertCount(3, $results);
    }
}
