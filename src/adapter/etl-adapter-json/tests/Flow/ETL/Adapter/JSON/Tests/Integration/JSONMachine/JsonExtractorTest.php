<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use function Flow\ETL\Adapter\JSON\{from_json};
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\{df, print_schema};
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Config, Flow, Row, Rows, Tests\FlowTestCase};
use Flow\Filesystem\Path;

final class JsonExtractorTest extends FlowTestCase
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
            ->read(from_json(__DIR__ . '/../../Fixtures/nested_timezones.json')->withPointer('/timezones', true))
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
        $extractor = new JsonExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/../../Fixtures/timezones.json'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(flow_context(\Flow\ETL\DSL\config())))
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new JsonExtractor(\Flow\Filesystem\DSL\path(__DIR__ . '/../../Fixtures/timezones.json'));

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
