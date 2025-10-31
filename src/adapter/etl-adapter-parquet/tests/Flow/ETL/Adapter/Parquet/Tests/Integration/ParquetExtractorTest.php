<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{config, flow_context};
use function Flow\Filesystem\DSL\{path, path_real};
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Reader;

final class ParquetExtractorTest extends FlowTestCase
{
    public function test_limit() : void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(flow_context(config())))
        );
    }

    public function test_reading_file_from_given_offset() : void
    {
        $totalRows = (new Reader())->read(__DIR__ . '/Fixtures/orders_1k.parquet')->metadata()->rowsNumber();

        $extractor = (from_parquet(path_real(__DIR__ . '/Fixtures/orders_1k.parquet')))->withOffset($totalRows - 100);

        self::assertCount(
            100,
            \iterator_to_array($extractor->extract(flow_context(config())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = from_parquet(path(__DIR__ . '/Fixtures/orders_1k.parquet'));

        $generator = $extractor->extract(flow_context(config()));

        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }
}
