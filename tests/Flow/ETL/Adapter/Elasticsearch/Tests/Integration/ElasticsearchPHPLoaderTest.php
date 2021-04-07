<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration;

use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHPLoader;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\Sha1IdFactory;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class ElasticsearchPHPLoaderTest extends TestCase
{
    public const INDEX_NAME = 'etl-test-index';

    protected function setUp() : void
    {
        parent::setUp();

        $this->elasticsearchContext->createIndex(self::INDEX_NAME);
    }

    protected function tearDown() : void
    {
        parent::tearDown();

        $this->elasticsearchContext->deleteIndex(self::INDEX_NAME);
    }

    public function test_integration_with_sha1_id_factory() : void
    {
        $loader = new ElasticsearchPHPLoader($this->elasticsearchContext->client(), 2, self::INDEX_NAME, new Sha1IdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Łukasz')
            ),
            Row::create(
                new Row\Entry\IntegerEntry('id', 2),
                new Row\Entry\StringEntry('name', 'Norbert')
            ),
            Row::create(
                new Row\Entry\IntegerEntry('id', 3),
                new Row\Entry\StringEntry('name', 'Dawid')
            ),
            Row::create(
                new Row\Entry\IntegerEntry('id', 4),
                new Row\Entry\StringEntry('name', 'Tomek')
            ),
        ));

        $params = [
            'index' => self::INDEX_NAME,
            'body'  => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        $this->assertSame(4, $response['hits']['total']['value']);

        $names = \array_map(fn (array $hit) : string => $hit['_source']['name'], $response['hits']['hits']);
        \sort($names);

        $this->assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }

    public function test_integration_with_json_entry() : void
    {
        $loader = new ElasticsearchPHPLoader($this->elasticsearchContext->client(), 2, self::INDEX_NAME, new Sha1IdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                Row\Entry\JsonEntry::object('json', ['foo' => 'bar'])
            ),
        ));

        $params = [
            'index' => self::INDEX_NAME,
            'body'  => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        $this->assertSame(1, $response['hits']['total']['value']);

        $json = \array_map(fn (array $hit) : array => $hit['_source']['json'], $response['hits']['hits']);

        $this->assertSame([['foo' => 'bar']], $json);
    }

    public function test_integration_with_entry_factory() : void
    {
        $loader = new ElasticsearchPHPLoader($this->elasticsearchContext->client(), 2, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', \sha1(\uniqid('id', true))),
                new Row\Entry\StringEntry('name', 'Łukasz')
            ),
            Row::create(
                new Row\Entry\StringEntry('id', \sha1(\uniqid('id', true))),
                new Row\Entry\StringEntry('name', 'Norbert')
            ),
            Row::create(
                new Row\Entry\StringEntry('id', \sha1(\uniqid('id', true))),
                new Row\Entry\StringEntry('name', 'Dawid')
            ),
            Row::create(
                new Row\Entry\StringEntry('id', \sha1(\uniqid('id', true))),
                new Row\Entry\StringEntry('name', 'Tomek')
            ),
        ));

        $params = [
            'index' => self::INDEX_NAME,
            'body'  => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        $this->assertSame(4, $response['hits']['total']['value']);

        $names = \array_map(fn (array $hit) : string => $hit['_source']['name'], $response['hits']['hits']);
        \sort($names);

        $this->assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }

    public function test_empty_rows() : void
    {
        $loader = new ElasticsearchPHPLoader($this->elasticsearchContext->client(), 2, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

        $loader->load(new Rows(
        ));

        $params = [
            'index' => self::INDEX_NAME,
            'body'  => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        $response = $this->elasticsearchContext->client()->search($params);

        $this->assertSame(0, $response['hits']['total']['value']);
    }
}
