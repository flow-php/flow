<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use Flow\ETL\Adapter\Seal\Tests\IntegrationTestCase;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;

use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\data_frame;

final class SealAllEntryTypesTest extends IntegrationTestCase
{
    public function test_indexing_pipeline_loads_all_flow_entry_types(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(
            (new FakeStaticOrdersExtractor())->schema(),
            'orders',
            'index',
        ));

        $report = data_frame()
            ->read(new FakeStaticOrdersExtractor(100))
            ->write(to_seal_upsert($engine, 'orders'))
            ->run(analyze: analyze());
        $this->sealContext()->refresh();

        static::assertNotNull($report);
        static::assertSame(100, $report->statistics()->totalRows());
        static::assertSame(100, $engine->countDocuments('orders'));
    }
}
