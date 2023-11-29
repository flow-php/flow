<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\from_array;
use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Config;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
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
            $this->assertInstanceOf(Row\Entry\StringEntry::class, $row->get('text'));
        }

        $this->assertSame(1024, $rows->count());
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

        $this->assertSame(2048, $total);
    }

    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/text_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_text($path))
            ->run();
        $extractor = new TextExtractor(Path::realpath($path));
        $extractor->changeLimit(2);

        $this->assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $path = \sys_get_temp_dir() . '/text_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_text($path))
            ->run();

        $extractor = new TextExtractor(Path::realpath($path));
        $generator = $extractor->extract(new FlowContext(Config::default()));

        $this->assertSame([['text' => '1']], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['text' => '2']], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['text' => '3']], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        $this->assertFalse($generator->valid());
    }
}
