<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Integration;

use Flow\ETL\Adapter\Seal\SealMetadata;
use Flow\ETL\Adapter\Seal\Tests\IntegrationTestCase;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Throwable;

use function array_values;
use function Flow\ETL\Adapter\Seal\from_seal;
use function Flow\ETL\Adapter\Seal\to_seal_schema;
use function Flow\ETL\Adapter\Seal\to_seal_upsert;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\uuid_schema;

final class SealKeysetExtractorTest extends IntegrationTestCase
{
    public function test_keyset_pagination_extracts_beyond_the_elasticsearch_result_window(): void
    {
        $count = 10_500;

        $definitions = array_values(FakeRandomOrdersExtractor::schema()->definitions());
        $definitions[] = uuid_schema(
            'cursor',
            false,
            SealMetadata::searchable()->merge(SealMetadata::filterable())->merge(SealMetadata::sortable()),
        );
        $flowSchema = schema(...$definitions);

        $engine = $this->sealContext()->engine(to_seal_schema($flowSchema, 'orders', 'order_id'));

        data_frame()
            ->read(new FakeRandomOrdersExtractor($count))
            ->withEntry('cursor', ref('order_id'))
            ->write(to_seal_upsert($engine, 'orders')->withBulkSize(2_000))
            ->run();
        $this->sealContext()->refresh();

        static::assertSame($count, $engine->countDocuments('orders'));

        $offsetFailed = false;

        try {
            data_frame()->read(from_seal($engine, 'orders'))->count();
        } catch (Throwable) {
            $offsetFailed = true;
        }

        static::assertTrue($offsetFailed, 'offset pagination is expected to fail past the result window');

        static::assertExtractedRowsCount($count, from_seal($engine, 'orders')->withKeysetPagination('cursor'));
    }
}
