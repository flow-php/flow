<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;

final class TextExtractorTest extends FlowTestCase
{
    public function test_extracting_text_file(): void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = data_frame()->read(from_text($path))->fetch();

        static::assertEquals(type_string(), $rows->schema()->get('text')->type());
        static::assertSame(1024, $rows->count());
    }

    public function test_limit(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));
        $extractor->changeLimit(2);

        static::assertCount(2, iterator_to_array($extractor->extract(flow_context(config()))));
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['text' => 'line a', 'date' => '2026-01-01'],
                ['text' => 'line b', 'date' => null],
            ],
            data_frame()
                ->read(from_text(__DIR__ . '/../Fixtures/cross_stream/*/data.txt'))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('text'), str_schema('_input_file_uri')),
            from_text(__DIR__ . '/../Fixtures/orders_flow.csv')->withMetadataColumns(true)->schema(),
        );
    }

    public function test_schema_describes_a_single_text_column(): void
    {
        static::assertEquals(schema(str_schema('text')), from_text(__DIR__ . '/../Fixtures/orders_flow.csv')->schema());
    }

    public function test_signal_stop(): void
    {
        $extractor = from_text(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_metadata_columns_extend_a_declared_schema(): void
    {
        static::assertEquals(
            schema(str_schema('line'), str_schema('_input_file_uri')),
            from_text(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv')
                ->withSchema(schema(str_schema('line')))
                ->withMetadataColumns(true)
                ->schema(),
        );
    }
}
