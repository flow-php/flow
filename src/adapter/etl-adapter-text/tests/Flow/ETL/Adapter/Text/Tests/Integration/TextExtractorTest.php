<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\ETL\Adapter\Text\{from_text};
use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Config, Flow, FlowContext, Row, Rows};
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class TextExtractorTest extends TestCase
{
    public function test_extracting_text_file() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = (new Flow())
            ->read(from_text($path))
            ->fetch();

        foreach ($rows as $row) {
            self::assertInstanceOf(Row\Entry\StringEntry::class, $row->get('text'));
        }

        self::assertSame(1024, $rows->count());
    }

    public function test_extracting_text_files_from_directory() : void
    {
        $extractor = from_text(
            [
                __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
                __DIR__ . '/../Fixtures/nested/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
            ],
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(new FlowContext(Config::default())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\StringEntry::class, $row->get('text'));
            });
            $total += $rows->count();
        }

        self::assertSame(2048, $total);
    }

    public function test_limit() : void
    {
        $extractor = new TextExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.csv'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new TextExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.csv'));

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
