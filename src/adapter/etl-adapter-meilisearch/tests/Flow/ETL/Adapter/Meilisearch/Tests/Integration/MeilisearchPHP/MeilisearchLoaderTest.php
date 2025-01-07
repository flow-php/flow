<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Integration\MeilisearchPHP;

use function Flow\ETL\Adapter\Meilisearch\{to_meilisearch_bulk_index, to_meilisearch_bulk_update};
use function Flow\ETL\DSL\string_entry;
use Flow\ETL\Adapter\Meilisearch\Tests\Context\MeilisearchContext;
use Flow\ETL\{Config, FlowContext, Row, Rows, Tests\FlowTestCase};

final class MeilisearchLoaderTest extends FlowTestCase
{
    private const INDEX_NAME = 'etl-test-index';

    private MeilisearchContext $meilisearchContext;

    protected function setUp() : void
    {
        $this->meilisearchContext = new MeilisearchContext(\getenv('MEILISEARCH_URL'), \getenv('MEILISEARCH_API_KEY'));
        $this->meilisearchContext->createIndex(self::INDEX_NAME);
    }

    protected function tearDown() : void
    {
        $this->meilisearchContext->deleteIndex(self::INDEX_NAME);
    }

    public function test_empty_rows() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(), new FlowContext(Config::default()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('', ['page' => 1]);

        self::assertCount(0, $response->getHits());
    }

    public function test_integration_with_entry_factory() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(
            Row::create(
                string_entry('id', \sha1('id' . \Flow\ETL\DSL\generate_random_string())),
                string_entry('name', 'Łukasz')
            ),
            Row::create(
                string_entry('id', \sha1('id' . \Flow\ETL\DSL\generate_random_string())),
                string_entry('name', 'Norbert')
            ),
            Row::create(
                string_entry('id', \sha1('id' . \Flow\ETL\DSL\generate_random_string())),
                string_entry('name', 'Dawid')
            ),
            Row::create(
                string_entry('id', \sha1('id' . \Flow\ETL\DSL\generate_random_string())),
                string_entry('name', 'Tomek')
            ),
        ), new FlowContext(Config::default()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('');

        self::assertSame(4, $response->getEstimatedTotalHits());

        $names = \array_map(static fn (array $hit) : string => $hit['name'], $response->getHits());
        \sort($names);

        self::assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }

    public function test_integration_with_json_entry() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                Row\Entry\JsonEntry::object('json', ['foo' => 'bar'])
            ),
        ), new FlowContext(Config::default()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('');

        self::assertSame(1, $response->getEstimatedTotalHits());

        $json = \array_map(static fn (array $hit) : array => $hit['json'], $response->getHits());

        self::assertSame([['foo' => 'bar']], $json);
    }

    public function test_integration_with_partial_update_id_factory() : void
    {
        $insertLoader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $insertLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                string_entry('name', 'Some Name'),
                string_entry('status', 'NEW'),
                new Row\Entry\DateTimeEntry('updated_at', new \DateTimeImmutable('2022-01-01 00:00:00'))
            ),
        ), new FlowContext(Config::default()));

        $updateLoader = to_meilisearch_bulk_update($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $updateLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                string_entry('name', 'Other Name'),
            ),
        ), new FlowContext(Config::default()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('');

        self::assertSame(1, $response->getEstimatedTotalHits());

        $data = \array_map(static fn (array $hit) : array => $hit, $response->getHits());

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
}
