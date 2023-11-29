<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Config;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class CSVExtractorTest extends TestCase
{
    public function test_extracting_csv_empty_columns_as_empty_strings() : void
    {
        $extractor = from_csv(
            $path = Path::realpath(__DIR__ . '/../Fixtures/file_with_empty_columns.csv'),
            empty_to_null: false,
        );

        $this->assertSame(
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
                \iterator_to_array($extractor->extract(new FlowContext(Config::builder()->putInputIntoRows()->build())))
            )
        );
    }

    public function test_extracting_csv_empty_columns_as_null() : void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/file_with_empty_columns.csv'
        );

        $this->assertSame(
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
                \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
            )
        );
    }

    public function test_extracting_csv_files_from_directory_recursively() : void
    {
        $extractor = from_csv(
            [
                Path::realpath(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv'),
                Path::realpath(__DIR__ . '/../Fixtures/nested/annual-enterprise-survey-2019-financial-year-provisional-csv.csv'),
            ],
            false
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(new FlowContext(Config::default())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['e00', 'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(64892, $total);
    }

    public function test_extracting_csv_files_with_header() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = (new Flow())
            ->read(from_csv($path))
            ->fetch();

        foreach ($rows as $row) {
            $this->assertSame(
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

        $this->assertSame(32445, $rows->count());
    }

    public function test_extracting_csv_files_without_header() : void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
            false
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(new FlowContext(Config::default())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['e00', 'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(32446, $total);
    }

    public function test_extracting_csv_with_corrupted_row() : void
    {
        $rows = (new Flow())
            ->extract(from_csv(__DIR__ . '/../Fixtures/corrupted_row.csv'))
            ->fetch();

        $this->assertSame(3, $rows->count());
    }

    public function test_extracting_csv_with_more_columns_than_headers() : void
    {
        $extractor = from_csv(
            __DIR__ . '/../Fixtures/more_columns_than_headers.csv'
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(new FlowContext(Config::default())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['id', 'name'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_headers_than_columns() : void
    {
        $extractor = from_csv(
            Path::realpath(__DIR__ . '/../Fixtures/more_headers_than_columns.csv')
        );

        $total = 0;

        /** @var Rows $rows */
        foreach ($extractor->extract(new FlowContext(Config::default())) as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertSame(
                    ['id', 'name', 'active'],
                    \array_keys($row->toArray())
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_splits_rows() : void
    {
        $this->assertCount(
            2,
            (new Flow())
                ->read(from_csv(__DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv'))
                ->fetch()
                ->toArray(),
            'Long line was broken down into two rows.'
        );
    }

    public function test_extracting_csv_with_more_than_1000_characters_per_line_with_increased_read_in_line_option() : void
    {
        $this->assertCount(
            1,
            (new Flow())
                ->read(from_csv(__DIR__ . '/../Fixtures/more_than_1000_characters_per_line.csv', characters_read_in_line: 2000))
                ->fetch()
                ->toArray(),
            'Long line was read as one row.'
        );
    }

    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/csv_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_csv($path))
            ->run();

        $extractor = new CSVExtractor(Path::realpath($path));
        $extractor->changeLimit(2);

        $this->assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_loading_data_from_all_partitions() : void
    {
        $this->assertSame(
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
            (new Flow())
                ->read(from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv'))
                ->withEntry('id', ref('id')->cast('int'))
                ->sortBy(ref('id'))
                ->fetch()
                ->toArray()
        );
    }

    public function test_loading_data_from_all_with_local_fs() : void
    {
        $this->assertSame(
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
            (new Flow((new ConfigBuilder())->filesystem(new LocalFilesystem())))
                ->read(from_csv(__DIR__ . '/../Fixtures/partitioned/group=*/*.csv'))
                ->withEntry('id', ref('id')->cast('int'))
                ->sortBy(ref('id'))
                ->fetch()
                ->toArray()
        );
    }

    public function test_signal_stop() : void
    {
        $path = \sys_get_temp_dir() . '/csv_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(from_array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(to_csv($path))
            ->run();

        $extractor = new CSVExtractor(Path::realpath($path));

        $generator = $extractor->extract(new FlowContext(Config::default()));

        $this->assertSame([['id' => '1']], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => '2']], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => '3']], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        $this->assertFalse($generator->valid());
    }
}
