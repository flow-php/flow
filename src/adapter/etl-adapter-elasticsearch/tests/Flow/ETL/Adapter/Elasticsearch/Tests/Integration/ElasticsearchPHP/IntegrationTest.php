<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration\ElasticsearchPHP;

use function Flow\ETL\Adapter\Elasticsearch\{es_hits_to_rows, from_es, to_es_bulk_index};
use function Flow\ETL\DSL\{bool_entry, int_entry, string_entry};
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\{Flow, Row, Rows};

final class IntegrationTest extends TestCase
{
    public const DESTINATION_INDEX = 'etl-test-destination-index';

    public const SOURCE_INDEX = 'etl-test-source-index';

    protected function setUp() : void
    {
        parent::setUp();

        $this->elasticsearchContext->deleteIndex(self::SOURCE_INDEX);
        $this->elasticsearchContext->createIndex(self::SOURCE_INDEX);
        $this->elasticsearchContext->deleteIndex(self::DESTINATION_INDEX);
        $this->elasticsearchContext->createIndex(self::DESTINATION_INDEX);
    }

    protected function tearDown() : void
    {
        parent::tearDown();

        $this->elasticsearchContext->deleteIndex(self::SOURCE_INDEX);
        $this->elasticsearchContext->deleteIndex(self::DESTINATION_INDEX);
    }

    public function test_loading_and_extraction_with_limit_and_transformation() : void
    {
        $this->elasticsearchContext->loadRows(
            new Rows(
                ...\array_map(
                    static fn (int $i) : Row => Row::create(
                        string_entry('id', \sha1((string) $i)),
                        int_entry('position', $i),
                        string_entry('name', 'id_' . $i),
                        bool_entry('active', false)
                    ),
                    \range(1, 2005)
                ),
            ),
            self::SOURCE_INDEX,
            new EntryIdFactory('id')
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

        $results = (new Flow())
            ->extract(from_es($this->elasticsearchContext->clientConfig(), $params))
            ->rows(es_hits_to_rows())
            ->limit($limit = 20)
            ->load(
                to_es_bulk_index(
                    $this->elasticsearchContext->clientConfig(),
                    index: self::DESTINATION_INDEX,
                    id_factory: new EntryIdFactory('id')
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
