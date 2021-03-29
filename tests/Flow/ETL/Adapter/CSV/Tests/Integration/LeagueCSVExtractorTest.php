<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\LeagueCSVExtractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use League\Csv\Reader;
use PHPUnit\Framework\TestCase;

final class LeagueCSVExtractorTest extends TestCase
{
    public function test_extracting_csv_files_with_header() : void
    {
        $reader = Reader::createFromPath(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv');
        $reader->setHeaderOffset(0);

        $extractor = new LeagueCSVExtractor($reader, 5);

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
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
            });
            $total += $rows->count();
        }

        $this->assertSame(32445, $total);
    }

    public function test_extracting_csv_files_without_header() : void
    {
        $reader = Reader::createFromPath(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv');

        $extractor = new LeagueCSVExtractor($reader, 5);

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $row->get('row'));
                $this->assertSame(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                    \array_keys($row->valueOf('row'))
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(32446, $total);
    }
}
