<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\Filesystem\DSL\path_real;
use function max;

final class ChainedBatchSizesTest extends FlowTestCase
{
    public function test_each_chained_source_keeps_its_own_batch_size(): void
    {
        $fixtures = __DIR__ . '/../../../../../../../../adapter';
        $csv = from_csv(path_real($fixtures
        . '/etl-adapter-csv/tests/Flow/ETL/Adapter/CSV/Tests/Fixtures/orders_flow.csv'));
        $json = from_json(path_real($fixtures
        . '/etl-adapter-json/tests/Flow/ETL/Adapter/JSON/Tests/Fixtures/timezones.json'));

        $csvRows = 0;

        foreach ($csv->extract(flow_context()) as $rows) {
            $csvRows += $rows->count();
        }

        $read = 0;
        $largestFromJson = 0;

        foreach (from_all($csv->withBatchSize(10), $json->withBatchSize(500))->extract(flow_context()) as $rows) {
            if ($read < $csvRows) {
                static::assertLessThanOrEqual(10, $rows->count());
            } else {
                static::assertLessThanOrEqual(500, $rows->count());
                $largestFromJson = max($largestFromJson, $rows->count());
            }

            $read += $rows->count();
        }

        // above the default of 100: the second child batched at its own size, not at the first one's
        static::assertGreaterThan(100, $largestFromJson);
    }
}
