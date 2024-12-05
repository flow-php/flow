<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Config, FlowContext};
use Flow\Filesystem\Path;
use Flow\Parquet\{Options, Reader};
use PHPUnit\Framework\TestCase;

final class ParquetExtractorTest extends TestCase
{
    public function test_limit() : void
    {
        $extractor = new ParquetExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/Fixtures/orders_1k.parquet'), Options::default());
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_reading_file_from_given_offset() : void
    {
        $totalRows = (new Reader())->read(__DIR__ . '/Fixtures/orders_1k.parquet')->metadata()->rowsNumber();

        $extractor = (new ParquetExtractor(
            Path::realpath(__DIR__ . '/Fixtures/orders_1k.parquet'),
        ))->withOffset($totalRows - 100);

        self::assertCount(
            100,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new ParquetExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/Fixtures/orders_1k.parquet'), Options::default());

        $generator = $extractor->extract(new FlowContext(Config::default()));

        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }
}
