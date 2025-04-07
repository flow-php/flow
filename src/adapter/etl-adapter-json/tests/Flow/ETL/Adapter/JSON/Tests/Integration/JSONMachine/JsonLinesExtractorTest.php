<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use function Flow\ETL\Adapter\JSON\{from_json_lines};
use function Flow\ETL\DSL\{data_frame, flow_context};
use function Flow\ETL\DSL\{df, print_schema};
use Flow\ETL\Adapter\JSON\JSONMachine\JsonLinesExtractor;
use Flow\ETL\{Config, Row, Rows, Tests\FlowTestCase};
use Flow\ETL\Extractor\Signal;
use Flow\Filesystem\Path;

final class JsonLinesExtractorTest extends FlowTestCase
{
    public function test_broken() : void
    {
        $rows = (data_frame(Config::builder()->putInputIntoRows()))
            ->read(from_json_lines(__DIR__ . '/../../Fixtures/timezones.jsonl'))
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

    public function test_extracting_jsonl_from_local_file_stream_using_pointer() : void
    {
        $rows = (data_frame())
            ->read(from_json_lines(__DIR__ . '/../../Fixtures/nested_timezones.jsonl')->withPointer('/timezones', true))
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
                \array_keys($row->get('/timezones')->value())
            );
        }

        self::assertSame(247, $rows->count());
    }

    public function test_extracting_jsonl_from_local_file_stream_with_schema() : void
    {
        $schema = df()->read(from_json_lines(__DIR__ . '/../../Fixtures/timezones.jsonl'))
                    ->autoCast()
                    ->schema();

        $rows = df()
            ->read(from_json_lines(
                __DIR__ . '/../../Fixtures/timezones.jsonl',
            )->withSchema($schema))
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

    public function test_extracting_jsonl_from_local_file_string_uri() : void
    {
        $extractor = (new JsonLinesExtractor(Path::realpath(__DIR__ . '/../../Fixtures/timezones.jsonl')));

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(flow_context(\Flow\ETL\DSL\config())) as $rows) {
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
        $extractor = (new JsonLinesExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/../../Fixtures/timezones.jsonl')));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(flow_context(\Flow\ETL\DSL\config())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = (new JsonLinesExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/../../Fixtures/timezones.jsonl')));

        $generator = $extractor->extract(flow_context(\Flow\ETL\DSL\config()));

        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->next();
        self::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        self::assertFalse($generator->valid());
    }
}
