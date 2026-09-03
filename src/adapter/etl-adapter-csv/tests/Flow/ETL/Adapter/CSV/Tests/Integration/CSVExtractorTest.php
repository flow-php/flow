<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\Exception\SchemaDefinitionNotUniqueException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Tests\OperatingSystem;
use RuntimeException;

use function array_keys;
use function array_map;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\partition_types;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\schema_metadata;
use function Flow\ETL\DSL\schema_to_ascii;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;
use function sort;

final class CSVExtractorTest extends FlowTestCase
{
    use OperatingSystem;

    public function test_a_hive_null_partition_value_reads_back_as_null(): void
    {
        $dir = __DIR__ . '/var/test_a_hive_null_partition_value_reads_back_as_null';

        df()
            ->read(from_array(
                [['id' => 1, 'year' => null], ['id' => 2, 'year' => '2024']],
                schema(int_schema('id'), str_schema('year', nullable: true)),
            ))
            ->write(to_csv($dir . '/data.csv')->saveMode(overwrite())->partitionBy(partition_by('year')))
            ->run();

        $rows = df()->read(from_csv($dir . '/*/*.csv')->withSchema(schema(int_schema('id'))))->fetch();

        $years = array_map(static fn(Row $row): mixed => $row->get('year'), $rows->all());
        sort($years);

        static::assertSame([null, '2024'], $years);
    }

    public function test_declared_partition_types_reach_the_rows(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')
            ->withSchema(schema(int_schema('id'), str_schema('value')))
            ->partitionTypes(partition_types(group: type_integer()));

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
            static::assertSame(1, $rows->first()->get('group'));

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_partition_columns_are_typed_without_a_declared_schema(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')->partitionTypes(
            partition_types(group: type_integer()),
        );

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            static::assertSame(1, $rows->first()->get('group'));

            return;
        }

        static::fail('extractor yielded nothing');
    }

    public function test_a_source_column_named_input_file_uri_throws_instead_of_being_overwritten(): void
    {
        $this->expectException(SchemaDefinitionNotUniqueException::class);

        df()
            ->read(from_csv(__DIR__ . '/../Fixtures/metadata_column_collision.csv', schema: schema(
                str_schema('_input_file_uri'),
                str_schema('name'),
            ))->withMetadataColumns(true))
            ->fetch();
    }

    public function test_schema_appends_the_metadata_column(): void
    {
        static::assertEquals(
            schema(str_schema('name'), str_schema('_input_file_uri')),
            from_csv(
                __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
                schema: schema(str_schema('name')),
            )
                ->withMetadataColumns(true)
                ->schema(),
        );
    }

    public function test_schema_is_refused_when_it_was_not_declared(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot describe what it will produce before producing it');

        from_csv(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv')->schema();
    }

    public function test_schema_is_the_declared_one(): void
    {
        static::assertEquals(
            schema(str_schema('name')),
            from_csv(
                __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
                schema: schema(str_schema('name')),
            )->schema(),
        );
    }

    /**
     * Pins the half that is NOT closed: with no declared schema there is no Schema::add() to
     * refuse, and the row-value write still clobbers the source column. The value-write sites are
     * deliberately untouched in this phase.
     */
    public function test_without_a_declared_schema_the_metadata_column_still_overwrites_silently(): void
    {
        $rows = df()
            ->read(from_csv(__DIR__ . '/../Fixtures/metadata_column_collision.csv')->withMetadataColumns(true))
            ->fetch();

        static::assertStringEndsWith(
            'metadata_column_collision.csv',
            type_string()->assert($rows->first()->get('_input_file_uri')),
        );
    }

    public function test_bom_removal_utf16_be(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf16be_bom.csv'))
            ->withMetadataColumns(true);
        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf16be_bom.csv', "\xFE\xFF"));

        static::assertSame(
            [
                [
                    [
                        'id' => '2',
                        'name' => 'asd',
                        'reference' => '144',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf16_le(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf16le_bom.csv'))
            ->withMetadataColumns(true);
        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf16le_bom.csv', "\xFF\xFE"));

        static::assertSame(
            [
                [
                    [
                        'id' => '2',
                        'name' => 'asd',
                        'reference' => '144',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf32_be(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf32be_bom.csv'))
            ->withMetadataColumns(true);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf32be_bom.csv', "\x00\x00\xFE\xFF"));

        static::assertSame(
            [
                [
                    [
                        'id' => '2',
                        'name' => 'asd',
                        'reference' => '144',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf32_le(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf32le_bom.csv'))
            ->withMetadataColumns(true);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf32le_bom.csv', "\xFF\xFE\x00\x00"));

        static::assertSame(
            [
                [
                    [
                        'id' => '2',
                        'name' => 'asd',
                        'reference' => '144',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_bom_removal_utf8(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf8_bom.csv'))->withMetadataColumns(true);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf8_bom.csv', "\xEF\xBB\xBF"));

        static::assertSame(
            [
                [
                    [
                        'id' => '2',
                        'name' => 'asd',
                        'reference' => '144',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_extract_does_not_mutate_metadata_of_user_provided_schema(): void
    {
        $schema = schema(int_schema('id', metadata: schema_metadata(['primary_key' => true])), str_schema('value'));

        $before = $schema->normalize();

        $extractor = from_csv(__DIR__ . '/../Fixtures/cross_stream/*/data.csv', schema: $schema)->withMetadataColumns(
            true,
        );

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertSame($before, $schema->normalize());
    }

    public function test_extract_does_not_mutate_user_provided_schema(): void
    {
        $schema = schema(int_schema('id'), str_schema('value'));

        $extractor = from_csv(__DIR__ . '/../Fixtures/cross_stream/*/data.csv', schema: $schema)->withMetadataColumns(
            true,
        );

        df(Config::builder())->read($extractor)->run();
        df(Config::builder())->read($extractor)->run();

        static::assertNull($schema->findDefinition('date'));
        static::assertNull($schema->findDefinition('_input_file_uri'));
    }

    public function test_extracting_csv_empty_columns_as_empty_strings(): void
    {
        $extractor = from_csv(
            $path = path_real(__DIR__ . '/../Fixtures/file_with_empty_columns.csv'),
            empty_to_null: false,
        )->withMetadataColumns(true);

        static::assertSame(
            [
                [
                    [
                        'id' => '',
                        'name' => '',
                        'active' => 'false',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
                [
                    [
                        'id' => '1',
                        'name' => 'Norbert',
                        'active' => '',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    public function test_extracting_csv_empty_columns_as_null(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/file_with_empty_columns.csv');

        static::assertSame(
            [
                [
                    [
                        'id' => null,
                        'name' => null,
                        'active' => 'false',
                    ],
                ],
                [
                    [
                        'id' => '1',
                        'name' => 'Norbert',
                        'active' => null,
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(config()))),
            ),
        );
    }

    public function test_extracting_csv_empty_headers(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/file_with_empty_headers.csv');

        static::assertSame(
            [
                [
                    ['e00' => null, 'name' => null, 'active' => 'false'],
                ],
                [
                    ['e00' => '1', 'name' => 'Norbert', 'active' => null],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(config()))),
            ),
        );
    }

    public function test_extracting_csv_files_with_header(): void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = df()->read(from_csv($path))->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'Year',
                    'Industry_aggregation_NZSIOC',
                    'Industry_code_NZSIOC',
                    'Industry_name_NZSIOC',
                    'Units',
                    'Variable_code',
                    'Variable_name',
                    'Variable_category',
                    'Value',
                    'Industry_code_ANZSIC06',
                ],
                array_keys($row->toArray()),
            );
        }

        static::assertSame(998, $rows->count());
    }

    public function test_extracting_csv_files_with_schema(): void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = df()
            ->read(from_csv($path, schema: $schema = df()->read(from_csv($path))->autoCast()->schema()))
            ->fetch();

        foreach ($rows as $row) {
            static::assertSame(
                [
                    'Year',
                    'Industry_aggregation_NZSIOC',
                    'Industry_code_NZSIOC',
                    'Industry_name_NZSIOC',
                    'Units',
                    'Variable_code',
                    'Variable_name',
                    'Variable_category',
                    'Value',
                    'Industry_code_ANZSIC06',
                ],
                array_keys($row->toArray()),
            );
        }

        static::assertSame(998, $rows->count());
        static::assertEquals($schema, $rows->schema());

        static::assertSame(<<<'SCHEMA'
            schema
            |-- Year: integer
            |-- Industry_aggregation_NZSIOC: string
            |-- Industry_code_NZSIOC: string
            |-- Industry_name_NZSIOC: string
            |-- Units: string
            |-- Variable_code: string
            |-- Variable_name: string
            |-- Variable_category: string
            |-- Value: string
            |-- Industry_code_ANZSIC06: string

            SCHEMA, schema_to_ascii($rows->schema()));
    }

    public function test_extracting_csv_files_without_header(): void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
            false,
        );

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(
                    ['e00', 'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09'],
                    array_keys($row->toArray()),
                );
            }
            $total += $rows->count();
        }

        static::assertSame(999, $total);
    }

    public function test_extracting_csv_with_corrupted_row(): void
    {
        $rows = df()->extract(from_csv(__DIR__ . '/../Fixtures/corrupted_row.csv'))->fetch();

        static::assertSame(3, $rows->count());
    }

    public function test_extracting_csv_with_more_columns_than_headers(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/more_columns_than_headers.csv');

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(['id', 'name'], array_keys($row->toArray()));
            }
            $total += $rows->count();
        }

        static::assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_headers_than_columns(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../Fixtures/more_headers_than_columns.csv'));

        $total = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            foreach ($rows->all() as $row) {
                static::assertSame(['id', 'name', 'active'], array_keys($row->toArray()));
            }
            $total += $rows->count();
        }

        static::assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_splits_rows(): void
    {
        static::assertCount(
            1,
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv'))
                ->fetch()
                ->toArray(),
            'Long line was broken down into two rows.',
        );
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_with_increased_read_in_line_option(): void
    {
        static::assertCount(
            1,
            df()
                ->read(from_csv(
                    __DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv',
                    characters_read_in_line: 2000,
                ))
                ->fetch()
                ->toArray(),
            'Long line was read as one row.',
        );
    }

    public function test_extracting_csv_with_multiline_strings(): void
    {
        if ($this->isWindows()) {
            static::markTestSkipped('This test is failing on windows due to different new line characters.');
        }

        $extractor = from_csv(__DIR__ . '/../Fixtures/multiline_strings.csv');

        $rows = df()->read($extractor)->fetch();

        static::assertSame(1, $rows->count());

        $row = $rows->first();
        static::assertSame('ABBA', $row->get('artist'));
        static::assertSame("Ahe's My Kind Of Girl", $row->get('song'));
        static::assertSame('/a/abba/ahes+my+kind+of+girl_20598417.html', $row->get('link'));

        $expectedText = "Look at her face, it's a wonderful face  \nAnd it means something special to me  \nLook at the way that she smiles when she sees me  \nHow lucky can one fellow be?  \n  \nShe's just my kind of girl, she makes me feel fine  \nWho could ever believe that she could be mine?  \nShe's just my kind of girl, without her I'm blue  \nAnd if she ever leaves me what could I do, what could I do?  \n  \nAnd when we go for a walk in the park  \nAnd she holds me and squeezes my hand  \nWe'll go on walking for hours and talking  \nAbout all the things that we plan  \n  \nShe's just my kind of girl, she makes me feel fine  \nWho could ever believe that she could be mine?  \nShe's just my kind of girl, without her I'm blue  \nAnd if she ever leaves me what could I do, what could I do?\n\n";

        static::assertSame($expectedText, $row->get('text'));
    }

    public function test_limit(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));
        $extractor->changeLimit(2);

        static::assertCount(2, iterator_to_array($extractor->extract(flow_context(config()))));
    }

    /**
     * p3's divergence: the fixture's own header is `group,id,value`, so before D4 extract() emitted
     * `group` first while schema() never mentioned it at all. Both now emit the partition block last.
     */
    public function test_schema_and_extract_agree_on_partition_columns(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')->withSchema(schema(
            int_schema('group'),
            int_schema('id'),
            str_schema('value'),
        ));

        static::assertSame(
            ['id', 'value', 'group'],
            array_map(static fn(Row\Reference $ref): string => $ref->name(), $extractor->schema()->references()->all()),
        );

        foreach ($extractor->extract(flow_context(Config::builder()->build())) as $rows) {
            static::assertEquals($extractor->schema(), $rows->schema());
        }
    }

    public function test_schema_lists_the_path_once_per_instance(): void
    {
        $extractor = from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv')->withSchema(schema(
            int_schema('id'),
            str_schema('value'),
        ));

        static::assertEquals($extractor->schema(), $extractor->schema());
        static::assertSame(
            ['id', 'value', 'group'],
            array_map(static fn(Row\Reference $ref): string => $ref->name(), $extractor->schema()->references()->all()),
        );
    }

    public function test_loading_data_from_all_partitions(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'value' => 'a', 'group' => '1'],
                ['id' => 2, 'value' => 'b', 'group' => '1'],
                ['id' => 3, 'value' => 'c', 'group' => '1'],
                ['id' => 4, 'value' => 'd', 'group' => '1'],
                ['id' => 5, 'value' => 'e', 'group' => '2'],
                ['id' => 6, 'value' => 'f', 'group' => '2'],
                ['id' => 7, 'value' => 'g', 'group' => '2'],
                ['id' => 8, 'value' => 'h', 'group' => '2'],
            ],
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv'))
                ->withEntry('id', ref('id')->cast('int'))
                ->sortBy([ref('id')])
                ->fetch()
                ->toArray(),
        );
    }

    public function test_partition_columns_are_not_leaking_between_streams(): void
    {
        static::assertSame(
            [
                ['id' => 1, 'value' => 'a', 'date' => '2026-01-01'],
                ['id' => 2, 'value' => 'b', 'date' => null],
            ],
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/cross_stream/*/data.csv', schema: schema(
                    int_schema('id'),
                    str_schema('value'),
                )))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_signal_stop(): void
    {
        $extractor = from_csv(path_real(__DIR__ . '/../Fixtures/orders_flow.csv'));

        $generator = $extractor->extract(flow_context(config()));

        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->next();
        static::assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        static::assertFalse($generator->valid());
    }

    public function test_without_bom_removal_utf8(): void
    {
        $extractor = from_csv($path = path_real(__DIR__ . '/../Fixtures/with_utf8_bom.csv'))->withMetadataColumns(true);

        $extractor = $extractor->withBOMRemoval(false);

        static::assertTrue($this->ensureBOMExists(__DIR__ . '/../Fixtures/with_utf8_bom.csv', "\xEF\xBB\xBF"));

        static::assertSame(
            [
                [
                    [
                        "\xEF\xBB\xBFid" => '2',
                        'name' => 'asd',
                        'reference' => '144',
                        '_input_file_uri' => $path->uri(),
                    ],
                ],
            ],
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array($extractor->extract(flow_context(Config::builder()->build()))),
            ),
        );
    }

    private function ensureBOMExists(string $path, string $BOM): bool
    {
        $handle = fopen($path, 'rb');

        if ($handle === false) {
            throw new RuntimeException('Failed to open file: ' . $path);
        }

        $bomLength = strlen($BOM);

        if ($bomLength === 0) {
            fclose($handle);

            return true;
        }

        $contents = fread($handle, $bomLength);
        fclose($handle);

        return $contents === $BOM;
    }
}
