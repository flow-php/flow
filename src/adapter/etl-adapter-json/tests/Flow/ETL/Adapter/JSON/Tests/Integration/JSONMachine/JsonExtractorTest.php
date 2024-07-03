<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use function Flow\ETL\Adapter\JSON\{from_json, to_json};
use function Flow\ETL\DSL\{df, print_schema};
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Config, Flow, FlowContext, Row, Rows};
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class JsonExtractorTest extends TestCase
{
    public function test_extracting_json_from_local_file_stream() : void
    {
        $rows = (new Flow(Config::builder()->putInputIntoRows()))
            ->read(from_json(__DIR__ . '/../../Fixtures/timezones.json'))
            ->fetch();

        foreach ($rows as $row) {
            self::assertSame(
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

        self::assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_stream_using_pointer() : void
    {
        $rows = (new Flow())
            ->read(from_json(__DIR__ . '/../../Fixtures/nested_timezones.json', pointer: '/timezones'))
            ->fetch();

        foreach ($rows as $row) {
            self::assertSame(
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

        self::assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_stream_with_schema() : void
    {
        $rows = df()
            ->read(from_json(
                __DIR__ . '/../../Fixtures/timezones.json',
                schema: $schema = df()
                    ->read(from_json(__DIR__ . '/../../Fixtures/timezones.json'))
                    ->autoCast()
                    ->schema()
            ))
            ->fetch();

        foreach ($rows as $row) {
            self::assertSame(
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

        self::assertSame(247, $rows->count());
        self::assertEquals($schema, $rows->schema());
        self::assertSame(
            <<<'SCHEMA'
schema
|-- timezones: list<string>
|-- latlng: list<float>
|-- name: string
|-- country_code: string
|-- capital: ?string

SCHEMA
            ,
            print_schema($schema)
        );
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

        self::assertSame(247, $total);
    }

    public function test_limit() : void
    {
        $extractor = new JsonExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/../../Fixtures/timezones.json'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new JsonExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/../../Fixtures/timezones.json'));

        $generator = $extractor->extract(new FlowContext(Config::default()));

        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }

    public function test_using_pattern_path() : void
    {
        $this->expectExceptionMessage("JsonLoader path can't be pattern, given: /path/*/pattern.json");

        to_json(new Path('/path/*/pattern.json'));
    }
}
