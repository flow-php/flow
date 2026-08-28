<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Reader;

use function array_keys;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;

final class ParquetExtractorTest extends FlowTestCase
{
    public function test_limit(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'));
        $extractor->changeLimit(2);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(2, $extractedRows);
    }

    public function test_reading_file_from_given_offset(): void
    {
        $totalRows = (new Reader())
            ->read(__DIR__ . '/Fixtures/orders_1k.parquet')
            ->metadata()
            ->rowsNumber();

        $extractor = from_parquet(path_real(__DIR__ . '/Fixtures/orders_1k.parquet'))->withOffset($totalRows - 100);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(100, $extractedRows);
    }

    public function test_calling_schema_does_not_change_extraction(): void
    {
        $cold = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'))->withMetadataColumns(true);

        $warm = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'));
        $warm->schema();
        $warm->withMetadataColumns(true);

        static::assertEquals(
            iterator_to_array($cold->extract(flow_context(config()))),
            iterator_to_array($warm->extract(flow_context(config()))),
        );
    }

    public function test_extract_yields_the_metadata_column_schema_promises(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'), columns: ['email'])
            ->withMetadataColumns(true)
            ->withSchema(schema(str_schema('email')));

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            static::assertSame($extractor->schema()->references()->names(), $batch->first()->entries()->names());

            break;
        }
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertSame(
            ['order_id', 'email', '_input_file_uri'],
            array_keys(
                from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'), columns: ['order_id', 'email'])
                    ->withMetadataColumns(true)
                    ->schema()
                    ->definitions(),
            ),
        );
    }

    public function test_schema_comes_from_the_file_footer(): void
    {
        static::assertSame(
            ['order_id', 'created_at', 'updated_at', 'discount', 'email', 'customer', 'address', 'notes', 'items'],
            array_keys(from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'))->schema()->definitions()),
        );
    }

    public function test_schema_is_idempotent(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'))->withMetadataColumns(true);

        static::assertEquals($extractor->schema(), $extractor->schema());
    }

    public function test_schema_is_narrowed_down_to_selected_columns(): void
    {
        static::assertSame(
            ['order_id', 'email'],
            array_keys(
                from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'), columns: ['order_id', 'email'])
                    ->schema()
                    ->definitions(),
            ),
        );
    }

    public function test_signal_stop(): void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/Pagination/*.parquet'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }
}
