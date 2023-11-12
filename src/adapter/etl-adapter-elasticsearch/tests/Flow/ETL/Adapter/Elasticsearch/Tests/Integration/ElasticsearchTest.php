<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration;

use Flow\ETL\Adapter\Elasticsearch\Doubles\Spy\HttpClientSpy;
use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\DSL\Elasticsearch;
use Flow\ETL\DSL\From;
use Flow\ETL\Flow;

final class ElasticsearchTest extends TestCase
{
    public function test_batch_size_when_its_not_explicitly_set() : void
    {
        if ($this->elasticsearchContext->version() <= 7) {
            $this->markTestSkipped('httpClient option is not accepted in Elasticsearch 7');
        }

        (new Flow())
            ->read(From::array([
                ['id' => 1, 'text' => 'lorem ipsum'],
                ['id' => 2, 'text' => 'lorem ipsum'],
                ['id' => 3, 'text' => 'lorem ipsum'],
                ['id' => 4, 'text' => 'lorem ipsum'],
                ['id' => 5, 'text' => 'lorem ipsum'],
                ['id' => 6, 'text' => 'lorem ipsum'],
            ]))
            ->write(
                Elasticsearch::bulk_index(
                    \array_merge(
                        $this->elasticsearchContext->clientConfig(),
                        ['httpClient' => $httpClient = new HttpClientSpy()]
                    ),
                    'test',
                    new EntryIdFactory('id')
                )
            )
            ->run();

        $this->assertCount(
            1,
            $httpClient->requests
        );
    }
}
