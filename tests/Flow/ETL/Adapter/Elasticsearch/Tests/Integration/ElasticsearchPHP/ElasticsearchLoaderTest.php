<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration\ElasticsearchPHP;

use Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP\ElasticsearchLoader;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\Sha1IdFactory;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\Serializer\CompressingSerializer;
use Flow\Serializer\NativePHPSerializer;

final class ElasticsearchLoaderTest extends TestCase
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
        $loader = new ElasticsearchLoader($this->elasticsearchContext->clientConfig(), 2, self::INDEX_NAME, new Sha1IdFactory('id'), ['refresh' => true]);

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

    public function test_integration_with_serialization() : void
    {
        $serializer = new CompressingSerializer(new NativePHPSerializer());

        $loaderSerialized = $serializer->serialize(
            new ElasticsearchLoader($this->elasticsearchContext->clientConfig(), 2, self::INDEX_NAME, new Sha1IdFactory('id'), ['refresh' => true])
        );

        $serializer->unserialize($loaderSerialized)->load(new Rows(
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

    public function test_integration_with_json_entry() : void
    {
        $loader = new ElasticsearchLoader($this->elasticsearchContext->clientConfig(), 2, self::INDEX_NAME, new Sha1IdFactory('id'), ['refresh' => true]);

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
        $loader = new ElasticsearchLoader($this->elasticsearchContext->clientConfig(), 2, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

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
        $loader = new ElasticsearchLoader($this->elasticsearchContext->clientConfig(), 2, self::INDEX_NAME, new EntryIdFactory('id'), ['refresh' => true]);

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

    public function test_integration_with_partial_update_id_factory() : void
    {
        $insertLoader = new ElasticsearchLoader($this->elasticsearchContext->clientConfig(), 2, self::INDEX_NAME, new Sha1IdFactory('id'), ['refresh' => true]);

        $insertLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Some Name'),
                new Row\Entry\StringEntry('status', 'NEW'),
                new Row\Entry\DateTimeEntry('updated_at', new \DateTimeImmutable('2022-01-01 00:00:00'))
            ),
        ));

        $updateLoader = ElasticsearchLoader::update($this->elasticsearchContext->clientConfig(), 2, self::INDEX_NAME, new Sha1IdFactory('id'), ['refresh' => true]);

        $updateLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Other Name'),
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

        $data = \array_map(fn (array $hit) : array => $hit['_source'], $response['hits']['hits']);

        $this->assertSame(
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
}
