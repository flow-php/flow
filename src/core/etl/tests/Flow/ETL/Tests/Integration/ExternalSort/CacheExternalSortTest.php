<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\ExternalSort;

use function Flow\ETL\DSL\{array_to_rows, config_builder, flow_context, ref};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Cache\InMemoryCache;
use Flow\ETL\ExternalSort\CacheExternalSort;

final class CacheExternalSortTest extends TestCase
{
    public function test_memory_implementation_of_external_sort_algorithm() : void
    {
        $input = [];

        for ($j = 10; $j > 0; $j--) {
            for ($i = 10; $i > 0; $i--) {
                $input[] = ['id' => str_pad((string) $j, 5, '0', STR_PAD_LEFT) . '-' . str_pad((string) $i, 3, '0', STR_PAD_LEFT)];
            }
        }

        $randomizedInput = $input;
        \shuffle($randomizedInput);

        $cache = new InMemoryCache();

        foreach (array_to_rows($randomizedInput)->chunks(10) as $chunk) {
            $cache->add('cache_id', $chunk);
        }

        $sort = new CacheExternalSort(
            'cache_id',
            $cache,
        );

        $sortedOutput = \iterator_to_array($sort->sortBy(ref('id')->desc())->extract(flow_context(config_builder()->cache($cache)->build())));

        self::assertEquals(
            $input,
            \array_merge(...\array_map(
                fn ($row) => $row->toArray(),
                $sortedOutput
            ))
        );
    }
}
