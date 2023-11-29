<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\from_array;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Config;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class JsonExtractorTest extends TestCase
{
    public function test_extracting_json_from_local_file_stream() : void
    {
        $rows = (new Flow(Config::builder()->putInputIntoRows()))
            ->read(from_json(__DIR__ . '/../../Fixtures/timezones.json'))
            ->fetch();

        foreach ($rows as $row) {
            $this->assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                    '_input_file_uri',
                ],
                \array_keys($row->toArray())
            );
        }

        $this->assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_stream_using_pointer() : void
    {
        $rows = (new Flow())
            ->read(from_json(__DIR__ . '/../../Fixtures/nested_timezones.json', pointer: '/timezones'))
            ->fetch();

        foreach ($rows as $row) {
            $this->assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',

                ],
                \array_keys($row->toArray())
            );
        }

        $this->assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_string_uri() : void
    {
        $extractor = new JsonExtractor(Path::realpath(__DIR__ . '/../../Fixtures/timezones.json'));

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(new FlowContext(Config::default())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    [
                        'timezones',
                        'latlng',
                        'name',
                        'country_code',
                        'capital',

                    ],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(247, $total);
    }

    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/json_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_json($path))
            ->run();

        $extractor = new JsonExtractor(Path::realpath($path));
        $extractor->changeLimit(2);

        $this->assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $path = \sys_get_temp_dir() . '/json_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_json($path))
            ->run();

        $extractor = new JsonExtractor(Path::realpath($path));

        $generator = $extractor->extract(new FlowContext(Config::default()));

        $this->assertSame([['id' => 1]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => 2]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => 3]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        $this->assertFalse($generator->valid());
    }

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("JsonLoader path can't be pattern, given: /path/*/pattern.json");

        to_json(new Path('/path/*/pattern.json'));
    }
}
