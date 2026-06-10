<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use Flow\ETL\Adapter\Seal\Tests\SealTestCase;
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;

use function Flow\ETL\Adapter\Seal\from_seal;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\data_frame;

final class SealAllEntryTypesTest extends SealTestCase
{
    public function test_extracting_pipeline_reads_back_all_flow_entry_types(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(FakeStaticOrdersExtractor::schema(), 'orders', 'index'));

        data_frame()->read(new FakeStaticOrdersExtractor(100))->write(to_seal_upsert($engine, 'orders'))->run();

        $rows = data_frame()->read(from_seal($engine, 'orders'))->fetch()->toArray();

        static::assertCount(100, $rows);

        $order = null;

        foreach ($rows as $row) {
            if ($row['email'] === 'user-0@example.com') {
                $order = $row;

                break;
            }
        }

        static::assertNotNull($order);
        static::assertSame('John Doe 0', $order['customer']);

        /** @var array<string, mixed> $address */
        $address = $order['address'];
        static::assertSame('123 Main St, Apt 0', $address['street']);

        /** @var list<mixed> $notes */
        $notes = $order['notes'];
        static::assertCount(3, $notes);

        /** @var list<array<string, mixed>> $items */
        $items = $order['items'];
        static::assertSame('SKU_0001', $items[0]['sku']);
        static::assertSame(1, $items[0]['quantity']);
    }

    public function test_indexing_pipeline_loads_all_flow_entry_types(): void
    {
        $engine = $this->sealContext()->engine(to_seal_schema(FakeStaticOrdersExtractor::schema(), 'orders', 'index'));

        $report = data_frame()
            ->read(new FakeStaticOrdersExtractor(100))
            ->write(to_seal_upsert($engine, 'orders'))
            ->run(analyze: analyze());

        static::assertNotNull($report);
        static::assertSame(100, $report->statistics()->totalRows());
        static::assertSame(100, $engine->countDocuments('orders'));
    }
}
