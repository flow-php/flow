<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function iterator_to_array;
use function microtime;

final class GroupByScaleTest extends FlowIntegrationTestCase
{
    public function test_memory_and_filesystem_aggregation_match_at_100k(): void
    {
        $data = [];

        foreach ((new FakeRandomOrdersExtractor(100_000))->rawData() as $order) {
            $data[] = [
                'seller_id' => $order['seller_id'],
                'email' => $order['email'],
                'discount' => $order['discount'],
            ];
        }

        $pipeline = static fn(ConfigBuilder $config): array => iterator_to_array(
            data_frame($config)
                ->read(from_array($data))
                ->groupBy(ref('email'))
                ->aggregate(count(ref('email')), sum(ref('discount')))
                ->sortBy(ref('email')->asc())
                ->getEachAsArray(),
        );

        $memory = $pipeline(config_builder());

        $start = microtime(true);
        $filesystem = $pipeline(config_builder()->groupingFilesystem());
        $elapsed = microtime(true) - $start;

        static::assertSame($memory, $filesystem);

        static::assertLessThan(
            30.0,
            $elapsed,
            'filesystem aggregation at 100k regressed toward the old super-linear cliff',
        );
    }
}
