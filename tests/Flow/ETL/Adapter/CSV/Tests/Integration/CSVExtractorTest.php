<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\DSL\CSV;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Stream\LocalFile;
use PHPUnit\Framework\TestCase;

final class CSVExtractorTest extends TestCase
{
    public function test_extracting_csv_files_with_header() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = (new Flow())
            ->read(CSV::from(new LocalFile($path)))
            ->fetch();

        foreach ($rows as $row) {
            $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $row->get('row'));
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
                \array_keys($row->valueOf('row'))
            );
        }

        $this->assertSame(32445, $rows->count());
    }

    public function test_extracting_csv_files_without_header() : void
    {
        $extractor = CSV::from(
            __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv',
            5,
            false
        );

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $row->get('row'));
                $this->assertSame(
                    ['e00', 'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09'],
                    \array_keys($row->valueOf('row'))
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(32446, $total);
    }

    public function test_extracting_csv_files_from_directory_recursively() : void
    {
        $extractor = CSV::from(
            [
                new LocalFile(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv'),
                new LocalFile(__DIR__ . '/../Fixtures/nested/annual-enterprise-survey-2019-financial-year-provisional-csv.csv'),
            ],
            1000,
            false
        );

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $row->get('row'));
                $this->assertSame(
                    ['e00', 'e01', 'e02', 'e03', 'e04', 'e05', 'e06', 'e07', 'e08', 'e09'],
                    \array_keys($row->valueOf('row'))
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(64892, $total);
    }

    public function test_extracting_csv_with_more_columns_than_headers() : void
    {
        $extractor = CSV::from(
            new LocalFile(__DIR__ . '/../Fixtures/more_columns_than_headers.csv')
        );

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $row->get('row'));
                $this->assertSame(
                    ['id', 'name'],
                    \array_keys($row->valueOf('row'))
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(1, $total);
    }

    public function test_extracting_csv_with_more_headers_than_columns() : void
    {
        $extractor = CSV::from(
            new LocalFile(__DIR__ . '/../Fixtures/more_headers_than_columns.csv')
        );

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $row->get('row'));
                $this->assertSame(
                    ['id', 'name', 'active'],
                    \array_keys($row->valueOf('row'))
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(1, $total);
    }

    public function test_extracting_csv_empty_columns_as_null() : void
    {
        $extractor = CSV::from(
            new LocalFile(__DIR__ . '/../Fixtures/file_with_empty_columns.csv')
        );

        $this->assertSame(
            [
                [
                    'row' => [
                        'id' => null,
                        'name' => null,
                        'active' => 'false',
                    ],
                ],
                [
                    'row' => [
                        'id' => '1',
                        'name' => 'Norbert',
                        'active' => null,
                    ],
                ],
            ],
            \iterator_to_array($extractor->extract())[0]->toArray()
        );
    }

    public function test_extracting_csv_empty_columns_as_empty_strings() : void
    {
        $extractor = CSV::from(
            new LocalFile(__DIR__ . '/../Fixtures/file_with_empty_columns.csv'),
            empty_to_null: false
        );

        $this->assertSame(
            [
                [
                    'row' => [
                        'id' => '',
                        'name' => '',
                        'active' => 'false',
                    ],
                ],
                [
                    'row' => [
                        'id' => '1',
                        'name' => 'Norbert',
                        'active' => '',
                    ],
                ],
            ],
            \iterator_to_array($extractor->extract())[0]->toArray()
        );
    }
}
