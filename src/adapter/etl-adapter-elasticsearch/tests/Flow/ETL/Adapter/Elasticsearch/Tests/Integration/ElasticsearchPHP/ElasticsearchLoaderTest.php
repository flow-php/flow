<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration\ElasticsearchPHP;

use DateTimeImmutable;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\ElasticsearchTestCase;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\JsonEntry;

use function array_map;
use function Flow\ETL\Adapter\Elasticsearch\entry_id_factory;
use function Flow\ETL\Adapter\Elasticsearch\hash_id_factory;
use function Flow\ETL\Adapter\Elasticsearch\to_es_bulk_index;
use function Flow\ETL\Adapter\Elasticsearch\to_es_bulk_update;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\generate_random_string;
use function Flow\ETL\DSL\integer_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;
use function sha1;
use function sort;

final class ElasticsearchLoaderTest extends ElasticsearchTestCase
{
    public const INDEX_NAME = 'etl-test-index';

    protected function setUp(): void
    {
        parent::setUp();

        $this->elasticsearchContext->createIndex(self::INDEX_NAME);
    }

    protected function tearDown(): void
    {
        $this->elasticsearchContext->deleteIndex(self::INDEX_NAME);
    }

    public function test_empty_rows(): void
    {
        $loader = to_es_bulk_index(
            $this->elasticsearchContext->clientConfig(),
            self::INDEX_NAME,
            entry_id_factory('id'),
            ['refresh' => true],
        );

        $loader->load(rows(), flow_context(config()));

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-method-access
        // @mago-ignore analysis:mixed-assignment
        $response = $this->elasticsearchContext->client()->search($params);

        /** @var array{hits: array{total: array{value: int}, hits: array<int, array{_source: array<string, mixed>}>}} $response */
        static::assertSame(0, $response['hits']['total']['value']);
    }

    public function test_integration_with_entry_factory(): void
    {
        $loader = to_es_bulk_index(
            $this->elasticsearchContext->clientConfig(),
            self::INDEX_NAME,
            entry_id_factory('id'),
            ['refresh' => true],
        );

        $loader->load(
            rows(
                row(string_entry('id', sha1('id' . generate_random_string())), string_entry('name', 'Łukasz')),
                row(string_entry('id', sha1('id' . generate_random_string())), string_entry('name', 'Norbert')),
                row(string_entry('id', sha1('id' . generate_random_string())), string_entry('name', 'Dawid')),
                row(string_entry('id', sha1('id' . generate_random_string())), string_entry('name', 'Tomek')),
            ),
            flow_context(config()),
        );

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-method-access
        // @mago-ignore analysis:mixed-assignment
        $response = $this->elasticsearchContext->client()->search($params);

        /** @var array{hits: array{total: array{value: int}, hits: array<int, array{_source: array<string, mixed>}>}} $response */
        static::assertSame(4, $response['hits']['total']['value']);

        /** @var array<int, array{_source: array{name: string}}> $hits */
        $hits = $response['hits']['hits'];
        $names = array_map(static fn(array $hit): string => $hit['_source']['name'], $hits);
        sort($names);

        static::assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }

    public function test_integration_with_json_entry(): void
    {
        $loader = to_es_bulk_index(
            $this->elasticsearchContext->clientConfig(),
            self::INDEX_NAME,
            hash_id_factory('id'),
            ['refresh' => true],
        );

        $loader->load(
            rows(row(integer_entry('id', 1), JsonEntry::object('json', ['foo' => 'bar']))),
            flow_context(config()),
        );

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-method-access
        // @mago-ignore analysis:mixed-assignment
        $response = $this->elasticsearchContext->client()->search($params);

        /** @var array{hits: array{total: array{value: int}, hits: array<int, array{_source: array<string, mixed>}>}} $response */
        static::assertSame(1, $response['hits']['total']['value']);

        /** @var array<int, array{_source: array{json: array<string, mixed>}}> $hits */
        $hits = $response['hits']['hits'];
        $json = array_map(static fn(array $hit): array => $hit['_source']['json'], $hits);

        static::assertSame([['foo' => 'bar']], $json);
    }

    public function test_integration_with_partial_update_id_factory(): void
    {
        $insertLoader = to_es_bulk_index(
            $this->elasticsearchContext->clientConfig(),
            self::INDEX_NAME,
            hash_id_factory('id'),
            ['refresh' => true],
        );

        $insertLoader->load(
            rows(row(
                integer_entry('id', 1),
                string_entry('name', 'Some Name'),
                string_entry('status', 'NEW'),
                new DateTimeEntry('updated_at', new DateTimeImmutable('2022-01-01 00:00:00')),
            )),
            flow_context(config()),
        );

        $updateLoader = to_es_bulk_update(
            $this->elasticsearchContext->clientConfig(),
            self::INDEX_NAME,
            hash_id_factory('id'),
            ['refresh' => true],
        );

        $updateLoader->load(
            rows(row(integer_entry('id', 1), string_entry('name', 'Other Name'))),
            flow_context(config()),
        );

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-method-access
        // @mago-ignore analysis:mixed-assignment
        $response = $this->elasticsearchContext->client()->search($params);

        /** @var array{hits: array{total: array{value: int}, hits: array<int, array{_source: array<string, mixed>}>}} $response */
        static::assertSame(1, $response['hits']['total']['value']);

        $hits = $response['hits']['hits'];
        $data = array_map(static fn(array $hit): array => $hit['_source'], $hits);

        static::assertSame(
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
            $data,
        );
    }

    public function test_integration_with_sha1_id_factory(): void
    {
        $loader = to_es_bulk_index(
            $this->elasticsearchContext->clientConfig(),
            self::INDEX_NAME,
            hash_id_factory('id'),
            ['refresh' => true],
        );

        $loader->load(
            rows(
                row(integer_entry('id', 1), string_entry('name', 'Łukasz')),
                row(integer_entry('id', 2), string_entry('name', 'Norbert')),
                row(integer_entry('id', 3), string_entry('name', 'Dawid')),
                row(integer_entry('id', 4), string_entry('name', 'Tomek')),
            ),
            flow_context(config()),
        );

        $params = [
            'index' => self::INDEX_NAME,
            'body' => [
                'query' => [
                    'match_all' => ['boost' => 1.0],
                ],
            ],
        ];

        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-method-access
        // @mago-ignore analysis:mixed-assignment
        $response = $this->elasticsearchContext->client()->search($params);

        /** @var array{hits: array{total: array{value: int}, hits: array<int, array{_source: array<string, mixed>}>}} $response */
        static::assertSame(4, $response['hits']['total']['value']);

        /** @var array<int, array{_source: array{name: string}}> $hits */
        $hits = $response['hits']['hits'];
        $names = array_map(static fn(array $hit): string => $hit['_source']['name'], $hits);
        sort($names);

        static::assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }
}
