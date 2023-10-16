<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Config;
use Flow\ETL\DSL\Json;
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
            ->read(Json::from(__DIR__ . '/../../Fixtures/timezones.json', 5))
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
            ->read(Json::from(__DIR__ . '/../../Fixtures/nested_timezones.json', 5, pointer: '/timezones'))
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
        $extractor = new JsonExtractor(Path::realpath(__DIR__ . '/../../Fixtures/timezones.json'), 5);

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

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("JsonLoader path can't be pattern, given: /path/*/pattern.json");

        Json::to(new Path('/path/*/pattern.json'));
    }
}
