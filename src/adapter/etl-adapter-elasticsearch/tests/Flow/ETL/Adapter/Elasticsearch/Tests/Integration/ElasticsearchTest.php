<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration;

use Flow\ETL\Adapter\Elasticsearch\EntryIdFactory\EntryIdFactory;
use Flow\ETL\Adapter\Elasticsearch\Tests\Doubles\Spy\HttpClientSpy;

use function array_merge;
use function Flow\ETL\Adapter\Elasticsearch\to_es_bulk_index;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

final class ElasticsearchTest extends ElasticsearchTestCase
{
    public function test_batch_size_when_its_not_explicitly_set(): void
    {
        if ($this->elasticsearchContext->version() <= 7) {
            static::markTestSkipped('httpClient option is not accepted in Elasticsearch 7');
        }

        data_frame()
            ->read(from_array([
                ['id' => 1, 'text' => 'lorem ipsum'],
                ['id' => 2, 'text' => 'lorem ipsum'],
                ['id' => 3, 'text' => 'lorem ipsum'],
                ['id' => 4, 'text' => 'lorem ipsum'],
                ['id' => 5, 'text' => 'lorem ipsum'],
                ['id' => 6, 'text' => 'lorem ipsum'],
            ]))
            // @mago-ignore analysis:possibly-invalid-argument
            ->write(to_es_bulk_index(array_merge($this->elasticsearchContext->clientConfig(), [
                'httpClient' => $httpClient = new HttpClientSpy(),
            ]), 'test', new EntryIdFactory('id')))
            ->run();

        static::assertCount(1, $httpClient->requests);
    }
}
