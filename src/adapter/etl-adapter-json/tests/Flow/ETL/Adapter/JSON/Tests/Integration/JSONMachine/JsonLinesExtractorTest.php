<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Config;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\JSON\from_json_lines;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\schema_to_ascii;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;

final class JsonLinesExtractorTest extends FlowTestCase
{
    public function test_broken(): void
    {
        $rows = data_frame(Config::builder()->putInputIntoRows())
            ->read(from_json_lines(__DIR__ . '/../../Fixtures/timezones.jsonl'))
            ->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                    '_input_file_uri',
                ],
                \array_keys($row->toArray()),
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extracting_jsonl_from_local_file_stream_using_pointer(): void
    {
        $rows = data_frame()
            ->read(from_json_lines(__DIR__ . '/../../Fixtures/nested_timezones.jsonl')->withPointer('/timezones', true))
            ->fetch();

        foreach ($rows as $row) {
            $value = $row->get('/timezones')->value();
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                \is_array($value) ? \array_keys($value) : [],
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extracting_jsonl_from_local_file_stream_with_schema(): void
    {
        $schema = df()
            ->read(from_json_lines(__DIR__ . '/../../Fixtures/timezones.jsonl'))
            ->autoCast()
            ->schema();

        $rows = df()->read(from_json_lines(__DIR__ . '/../../Fixtures/timezones.jsonl')->withSchema($schema))->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                \array_keys($row->toArray()),
            );
        }

        static::assertSame(247, $rows->count());
        static::assertEquals($schema, $rows->schema());
        static::assertSame(<<<'SCHEMA'
            schema
            |-- timezones: list<string>
            |-- latlng: list<float>
            |-- name: string
            |-- country_code: string
            |-- capital: ?string

            SCHEMA, schema_to_ascii($schema));
    }

    public function test_extracting_jsonl_from_local_file_string_uri(): void
    {
        $extractor = from_json_lines(path_real(__DIR__ . '/../../Fixtures/timezones.jsonl'));

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(flow_context(\Flow\ETL\DSL\config())) as $rows) {
            $rows->each(function (Row $row): void {
                $this->assertSame(
                    [
                        'timezones',
                        'latlng',
                        'name',
                        'country_code',
                        'capital',
                    ],
                    \array_keys($row->toArray()),
                );
            });
            $total += $rows->count();
        }

        static::assertSame(247, $total);
    }

    public function test_limit(): void
    {
        $extractor = from_json_lines(path(__DIR__ . '/../../Fixtures/timezones.jsonl'));
        $extractor->changeLimit(2);

        static::assertCount(2, \iterator_to_array($extractor->extract(flow_context(\Flow\ETL\DSL\config()))));
    }

    public function test_signal_stop(): void
    {
        $extractor = from_json_lines(path(__DIR__ . '/../../Fixtures/timezones.jsonl'));

        $generator = $extractor->extract(flow_context(\Flow\ETL\DSL\config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }
}
