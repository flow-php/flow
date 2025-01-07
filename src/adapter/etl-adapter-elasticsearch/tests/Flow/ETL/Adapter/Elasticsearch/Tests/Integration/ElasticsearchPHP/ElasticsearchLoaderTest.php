<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration\ElasticsearchPHP;

use function Flow\ETL\Adapter\Elasticsearch\{entry_id_factory, hash_id_factory, to_es_bulk_index, to_es_bulk_update};
use function Flow\ETL\DSL\{generate_random_string, string_entry};
use Flow\ETL\{Adapter\Elasticsearch\Tests\Integration\ElasticsearchTestCase, Config, FlowContext, Row, Rows};

final class ElasticsearchLoaderTest extends ElasticsearchTestCase
{
    public const INDEX_NAME = 'etl-test-index';

    protected function setUp() : void
    {
        parent::setUp();

        $this->elasticsearchContext->createIndex(self::INDEX_NAME);
    }

    protected function tearDown() : void
    {
        $this->elasticsearchContext->deleteIndex(self::INDEX_NAME);
    }

    public function test_empty_rows() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, entry_id_factory('id'), ['refresh' => true]);

        $loader->load(new Rows(), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        self::assertSame(0, $response['hits']['total']['value']);
    }

    public function test_integration_with_entry_factory() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, entry_id_factory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            Row::create(
                string_entry('id', \sha1('id' . generate_random_string())),
                string_entry('name', 'Łukasz')
            ),
            Row::create(
                string_entry('id', \sha1('id' . generate_random_string())),
                string_entry('name', 'Norbert')
            ),
            Row::create(
                string_entry('id', \sha1('id' . generate_random_string())),
                string_entry('name', 'Dawid')
            ),
            Row::create(
                string_entry('id', \sha1('id' . generate_random_string())),
                string_entry('name', 'Tomek')
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        self::assertSame(4, $response['hits']['total']['value']);

        $names = \array_map(fn (array $hit) : string => $hit['_source']['name'], $response['hits']['hits']);
        \sort($names);

        self::assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }

    public function test_integration_with_json_entry() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, hash_id_factory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                Row\Entry\JsonEntry::object('json', ['foo' => 'bar'])
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        self::assertSame(1, $response['hits']['total']['value']);

        $json = \array_map(fn (array $hit) : array => $hit['_source']['json'], $response['hits']['hits']);

        self::assertSame([['foo' => 'bar']], $json);
    }

    public function test_integration_with_partial_update_id_factory() : void
    {
        $insertLoader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, hash_id_factory('id'), ['refresh' => true]);

        $insertLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                string_entry('name', 'Some Name'),
                string_entry('status', 'NEW'),
                new Row\Entry\DateTimeEntry('updated_at', new \DateTimeImmutable('2022-01-01 00:00:00'))
            ),
        ), new FlowContext(Config::default()));

        $updateLoader = to_es_bulk_update($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, hash_id_factory('id'), ['refresh' => true]);

        $updateLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                string_entry('name', 'Other Name'),
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        self::assertSame(1, $response['hits']['total']['value']);

        $data = \array_map(fn (array $hit) : array => $hit['_source'], $response['hits']['hits']);

        self::assertSame(
            [
                [
                    'id' => 1,
                    'name' => 'Other Name',
                    'status' => 'NEW',
                    'updated_at' => [
                        'date' => '2022-01-01 00:00:00.000000',
                        'timezone_type' => 3,
                        'timezone' => 'UTC',
                    ],
                ],
            ],
            $data
        );
    }

    public function test_integration_with_sha1_id_factory() : void
    {
        $loader = to_es_bulk_index($this->elasticsearchContext->clientConfig(), self::INDEX_NAME, hash_id_factory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                string_entry('name', 'Łukasz')
            ),
            Row::create(
                new Row\Entry\IntegerEntry('id', 2),
                string_entry('name', 'Norbert')
            ),
            Row::create(
                new Row\Entry\IntegerEntry('id', 3),
                string_entry('name', 'Dawid')
            ),
            Row::create(
                new Row\Entry\IntegerEntry('id', 4),
                string_entry('name', 'Tomek')
            ),
        ), new FlowContext(Config::default()));

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        self::assertSame(4, $response['hits']['total']['value']);

        $names = \array_map(fn (array $hit) : string => $hit['_source']['name'], $response['hits']['hits']);
        \sort($names);

        self::assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }
}
