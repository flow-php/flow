<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Integration\MeilisearchPHP;

use function Flow\ETL\Adapter\Meilisearch\{to_meilisearch_bulk_index, to_meilisearch_bulk_update};
use function Flow\ETL\DSL\{config, row, rows};
use function Flow\ETL\DSL\{flow_context, generate_random_string, integer_entry, string_entry};
use Flow\ETL\Adapter\Meilisearch\Tests\Context\MeilisearchContext;
use Flow\ETL\Row\Entry\{DateTimeEntry, JsonEntry};
use Flow\ETL\{Tests\FlowTestCase};

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
        $loader->load(rows(), flow_context(config()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('', ['page' => 1]);

        self::assertCount(0, $response->getHits());
    }

    public function test_integration_with_entry_factory() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(rows(row(string_entry('id', \sha1('id' . generate_random_string())), string_entry('name', 'Łukasz')), row(string_entry('id', \sha1('id' . generate_random_string())), string_entry('name', 'Norbert')), row(string_entry('id', \sha1('id' . generate_random_string())), string_entry('name', 'Dawid')), row(string_entry('id', \sha1('id' . generate_random_string())), string_entry('name', 'Tomek'))), flow_context(config()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('');

        self::assertSame(4, $response->getEstimatedTotalHits());

        $names = \array_map(static fn (array $hit) : string => $hit['name'], $response->getHits());
        \sort($names);

        self::assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
    }

    public function test_integration_with_json_entry() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(rows(row(integer_entry('id', 1), JsonEntry::object('json', ['foo' => 'bar']))), flow_context(config()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('');

        self::assertSame(1, $response->getEstimatedTotalHits());

        $json = \array_map(static fn (array $hit) : array => $hit['json'], $response->getHits());

        self::assertSame([['foo' => 'bar']], $json);
    }

    public function test_integration_with_partial_update_id_factory() : void
    {
        $insertLoader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $insertLoader->load(rows(row(integer_entry('id', 1), string_entry('name', 'Some Name'), string_entry('status', 'NEW'), new DateTimeEntry('updated_at', new \DateTimeImmutable('2022-01-01 00:00:00')))), flow_context(config()));

        $updateLoader = to_meilisearch_bulk_update($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $updateLoader->load(rows(row(integer_entry('id', 1), string_entry('name', 'Other Name'))), flow_context(config()));

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
