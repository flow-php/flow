<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration;

use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Row;

use function array_map;
use function Flow\ETL\Adapter\Elasticsearch\es_hits_to_rows;
use function Flow\ETL\Adapter\Elasticsearch\from_es;
use function Flow\ETL\Adapter\Elasticsearch\to_es_bulk_index;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\string_entry;
use function range;
use function sha1;

final class ElasticsearchIntegrationTest extends ElasticsearchTestCase
{
    public const DESTINATION_INDEX = 'etl-test-destination-index';

    public const SOURCE_INDEX = 'etl-test-source-index';

    protected function setUp(): void
    {
        parent::setUp();

        $this->elasticsearchContext->deleteIndex(self::SOURCE_INDEX);
        $this->elasticsearchContext->createIndex(self::SOURCE_INDEX);
        $this->elasticsearchContext->deleteIndex(self::DESTINATION_INDEX);
        $this->elasticsearchContext->createIndex(self::DESTINATION_INDEX);
    }

    protected function tearDown(): void
    {
        $this->elasticsearchContext->deleteIndex(self::SOURCE_INDEX);
        $this->elasticsearchContext->deleteIndex(self::DESTINATION_INDEX);
    }

    public function test_loading_and_extraction_with_limit_and_transformation(): void
    {
        $this->elasticsearchContext->loadRows(
            rows(...array_map(
                static fn(int $i): Row => row(
                    string_entry('id', sha1((string) $i)),
                    int_entry('position', $i),
                    string_entry('name', 'id_' . $i),
                    bool_entry('active', false),
                ),
                range(1, 2005),
            )),
            self::SOURCE_INDEX,
            new EntryIdFactory('id'),
        );

        $params = [
            'index' => self::SOURCE_INDEX,
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

        $results = data_frame()
            ->extract(from_es($this->elasticsearchContext->clientConfig(), $params))
            ->rows(es_hits_to_rows())
            ->limit($limit = 20)
            ->load(to_es_bulk_index(
                $this->elasticsearchContext->clientConfig(),
                index: self::DESTINATION_INDEX,
                id_factory: new EntryIdFactory('id'),
            ))
            ->fetch();

        static::assertCount($limit, $results);
        static::assertSame(
            array_map(
                static fn(int $i): array => [
                    'id' => sha1((string) $i),
                    'position' => $i,
                    'name' => 'id_' . $i,
                    'active' => false,
                ],
                range(1, $limit),
            ),
            $results->toArray(),
        );
    }
}
