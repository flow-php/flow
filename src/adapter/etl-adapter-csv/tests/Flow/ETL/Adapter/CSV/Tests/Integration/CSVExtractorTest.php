<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use function Flow\ETL\Adapter\CSV\{from_csv};
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\{df, print_schema, ref};
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\{Config, Row, Rows, Tests\FlowTestCase};
use Flow\Filesystem\Path;

final class CSVExtractorTest extends FlowTestCase
{
    public function test_extracting_csv_empty_columns_as_empty_strings() : void
    {
        $extractor = from_csv(
            $path = Path::realpath(__DIR__ . '/../Fixtures/file_with_empty_columns.csv'),
            empty_to_null: false,
        );

        self::assertSame(
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
            \array_map(
                static fn (Rows $r) => $r->toArray(),
                \iterator_to_array($extractor->extract(flow_context(Config::builder()->putInputIntoRows()->build())))
            )
        );
    }

    public function test_extracting_csv_empty_columns_as_null() : void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/file_with_empty_columns.csv'
        );

        self::assertSame(
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
            \array_map(
                static fn (Rows $r) => $r->toArray(),
                \iterator_to_array($extractor->extract(flow_context(\Flow\ETL\DSL\config())))
            )
        );
    }

    public function test_extracting_csv_empty_headers() : void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/file_with_empty_headers.csv'
        );

        self::assertSame(
            [
                [
                    ['e00' => null, 'name' => null, 'active' => 'false'],
                ],
                [
                    ['e00' => '1', 'name' => 'Norbert', 'active' => null],
                ],
            ],
            \array_map(
                fn (Rows $r) => $r->toArray(),
                \iterator_to_array($extractor->extract(flow_context(\Flow\ETL\DSL\config())))
            )
        );
    }

    public function test_extracting_csv_files_with_header() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = df()
            ->read(from_csv($path))
            ->fetch();

        foreach ($rows as $row) {
            self::assertSame(
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
                \array_keys($row->toArray())
            );
        }

        self::assertSame(998, $rows->count());
    }

    public function test_extracting_csv_files_with_schema() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = df()
            ->read(
                from_csv($path, schema: $schema = df()
                ->read(from_csv($path))
                ->autoCast()
                ->schema())
            )
            ->fetch();

        foreach ($rows as $row) {
            self::assertSame(
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
                \array_keys($row->toArray())
            );
        }

        self::assertSame(998, $rows->count());
        self::assertEquals($schema, $rows->schema());

        self::assertSame(
            <<<'SCHEMA'
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

SCHEMA,
            print_schema($rows->schema())
        );

    }

    public function test_extracting_csv_files_without_header() : void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
            false
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(flow_context(\Flow\ETL\DSL\config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['e00', 'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(999, $total);
    }

    public function test_extracting_csv_with_corrupted_row() : void
    {
        $rows = df()
            ->extract(from_csv(__DIR__ . '/../Fixtures/corrupted_row.csv'))
            ->fetch();

        self::assertSame(3, $rows->count());
    }

    public function test_extracting_csv_with_more_columns_than_headers() : void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/more_columns_than_headers.csv'
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(flow_context(\Flow\ETL\DSL\config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['id', 'name'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_headers_than_columns() : void
    {
        $extractor = from_csv(
            Path::realpath(__DIR__ . '/../Fixtures/more_headers_than_columns.csv')
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(flow_context(\Flow\ETL\DSL\config())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['id', 'name', 'active'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        self::assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_splits_rows() : void
    {
        self::assertCount(
            1,
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv'))
                ->fetch()
                ->toArray(),
            'Long line was broken down into two rows.'
        );
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_with_increased_read_in_line_option() : void
    {
        self::assertCount(
            1,
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv', characters_read_in_line: 2000))
                ->fetch()
                ->toArray(),
            'Long line was read as one row.'
        );
    }

    public function test_limit() : void
    {

        $extractor = new CSVExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.csv'));
        $extractor->changeLimit(2);

        self::assertCount(
            2,
            \iterator_to_array($extractor->extract(flow_context(\Flow\ETL\DSL\config())))
        );
    }

    public function test_loading_data_from_all_partitions() : void
    {
        self::assertSame(
            [
                ['group' => '1', 'id' => 1, 'value' => 'a'],
                ['group' => '1', 'id' => 2, 'value' => 'b'],
                ['group' => '1', 'id' => 3, 'value' => 'c'],
                ['group' => '1', 'id' => 4, 'value' => 'd'],
                ['group' => '2', 'id' => 5, 'value' => 'e'],
                ['group' => '2', 'id' => 6, 'value' => 'f'],
                ['group' => '2', 'id' => 7, 'value' => 'g'],
                ['group' => '2', 'id' => 8, 'value' => 'h'],
            ],
            df()
                ->read(from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv'))
                ->withEntry('id', ref('id')->cast('int'))
                ->sortBy(ref('id'))
                ->fetch()
                ->toArray()
        );
    }

    public function test_signal_stop() : void
    {
        $extractor = new CSVExtractor(Path::realpath(__DIR__ . '/../Fixtures/orders_flow.csv'));

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
