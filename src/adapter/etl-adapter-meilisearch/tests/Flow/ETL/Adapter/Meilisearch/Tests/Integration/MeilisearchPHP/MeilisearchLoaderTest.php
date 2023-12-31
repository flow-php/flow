<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Integration\MeilisearchPHP;

use function Flow\ETL\Adapter\Meilisearch\to_meilisearch_bulk_index;
use function Flow\ETL\Adapter\Meilisearch\to_meilisearch_bulk_update;
use Flow\ETL\Adapter\Meilisearch\Tests\Context\MeilisearchContext;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class MeilisearchLoaderTest extends TestCase
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
        parent::tearDown();

        $this->meilisearchContext->deleteIndex(self::INDEX_NAME);
    }

    public function test_empty_rows() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $loader->load(new Rows(), new FlowContext(Config::default()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('', ['page' => 1]);

        $this->assertCount(0, $response->getHits());
    }

    public function test_integration_with_entry_factory() : void
    {
        $loader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
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
        ), new FlowContext(Config::default()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('');

        $this->assertSame(4, $response->getEstimatedTotalHits());

        $names = \array_map(static fn (array $hit) : string => $hit['name'], $response->getHits());
        \sort($names);

        $this->assertSame(['Dawid', 'Norbert', 'Tomek', 'Łukasz'], $names);
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

        $this->assertSame(1, $response->getEstimatedTotalHits());

        $json = \array_map(static fn (array $hit) : array => $hit['json'], $response->getHits());

        $this->assertSame([['foo' => 'bar']], $json);
    }

    public function test_integration_with_partial_update_id_factory() : void
    {
        $insertLoader = to_meilisearch_bulk_index($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $insertLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Some Name'),
                new Row\Entry\StringEntry('status', 'NEW'),
                new Row\Entry\DateTimeEntry('updated_at', new \DateTimeImmutable('2022-01-01 00:00:00'))
            ),
        ), new FlowContext(Config::default()));

        $updateLoader = to_meilisearch_bulk_update($this->meilisearchContext->clientConfig(), self::INDEX_NAME);
        $updateLoader->load(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('name', 'Other Name'),
            ),
        ), new FlowContext(Config::default()));

        $response = $this->meilisearchContext->client()->index(self::INDEX_NAME)->search('');

        $this->assertSame(1, $response->getEstimatedTotalHits());

        $data = \array_map(static fn (array $hit) : array => $hit, $response->getHits());

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
