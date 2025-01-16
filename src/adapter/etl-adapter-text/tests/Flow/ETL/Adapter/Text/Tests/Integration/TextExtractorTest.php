<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\ETL\Adapter\Text\{from_text};
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\{Tests\FlowTestCase};
use Flow\Filesystem\Path;

final class TextExtractorTest extends FlowTestCase
{
    public function test_extracting_text_file() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = (data_frame())
            ->read(from_text($path))
            ->fetch();

        foreach ($rows as $row) {
            self::assertInstanceOf(StringEntry::class, $row->get('text'));
        }

        self::assertSame(1024, $rows->count());
    }

    public function test_limit() : void
    {
        $extractor = new TextExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.csv'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(flow_context(config())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new TextExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.csv'));

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
