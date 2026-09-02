<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Config;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_to_ascii;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_array;
use function iterator_to_array;

final class JsonExtractorTest extends FlowTestCase
{
    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(int_schema('id'), str_schema('value'));

        $extractor = from_json(
            __DIR__ . '/../../Fixtures/cross_stream/*/data.json',
            schema: $schema,
        )->withMetadataColumns(true);

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_extracting_json_from_local_file_stream(): void
    {
        $rows = data_frame(Config::builder())
            ->read(from_json(__DIR__ . '/../../Fixtures/timezones.json')->withMetadataColumns(true))
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
                array_keys($row->toArray()),
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_stream_using_pointer(): void
    {
        $rows = data_frame()
            ->read(from_json(__DIR__ . '/../../Fixtures/nested_timezones.json')->withPointer('/timezones', true))
            ->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                array_keys(type_array()->assert($row->get('/timezones'))),
            );
        }

        static::assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_stream_with_schema(): void
    {
        $rows = df()
            ->read(from_json(
                __DIR__ . '/../../Fixtures/timezones.json',
                schema: $schema = df()
                    ->read(from_json(__DIR__ . '/../../Fixtures/timezones.json'))
                    ->autoCast()
                    ->schema(),
            ))
            ->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'timezones',
                    'latlng',
                    'name',
                    'country_code',
                    'capital',
                ],
                array_keys($row->toArray()),
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

    public function test_extracting_json_from_local_file_string_uri(): void
    {
        $extractor = from_json(path_real(__DIR__ . '/../../Fixtures/timezones.json'));

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(
                    [
                        'timezones',
                        'latlng',
                        'name',
                        'country_code',
                        'capital',
                    ],
                    array_keys($row->toArray()),
                );
            }
            $total += $rows->count();
        }

        static::assertSame(247, $total);
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'value' => 'a', 'date' => '2026-01-01'],
                ['id' => 2, 'value' => 'b', 'date' => null],
            ],
            df()
                ->read(from_json(__DIR__ . '/../../Fixtures/cross_stream/*/data.json', schema: schema(
                    int_schema('id'),
                    str_schema('value'),
                )))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_limit(): void
    {
        $extractor = from_json(path(__DIR__ . '/../../Fixtures/timezones.json'));
        $extractor->changeLimit(2);

        static::assertCount(2, iterator_to_array($extractor->extract(flow_context(config()))));
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('order_id'), str_schema('_input_file_uri')),
            from_json(__DIR__ . '/../../Fixtures/orders_flow.json', schema: schema(str_schema('order_id')))
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_schema_is_refused_when_it_was_not_declared(): void
    {
        $this->expectException(SchemaNotDerivableException::class);

        from_json(__DIR__ . '/../../Fixtures/orders_flow.json')->schema();
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('order_id')),
            from_json(__DIR__ . '/../../Fixtures/orders_flow.json', schema: schema(str_schema('order_id')))->schema(),
        );
    }

    public function test_signal_stop(): void
    {
        $extractor = from_json(path(__DIR__ . '/../../Fixtures/timezones.json'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }
}
