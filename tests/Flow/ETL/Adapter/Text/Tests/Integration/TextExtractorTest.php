<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\Tests\Integration;

use Flow\ETL\DSL\Text;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Stream\LocalFile;
use PHPUnit\Framework\TestCase;

final class TextExtractorTest extends TestCase
{
    public function test_extracting_text_file() : void
    {
        $path = __DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv';

        $rows = (new Flow())
            ->read(Text::from(new LocalFile($path)))
            ->fetch();

        foreach ($rows as $row) {
            $this->assertInstanceOf(Row\Entry\StringEntry::class, $row->get('row'));
        }

        $this->assertSame(32446, $rows->count());
    }

    public function test_extracting_text_files_from_directory() : void
    {
        $extractor = Text::from(
            [
                new LocalFile(__DIR__ . '/../Fixtures/annual-enterprise-survey-2019-financial-year-provisional-csv.csv'),
                new LocalFile(__DIR__ . '/../Fixtures/nested/annual-enterprise-survey-2019-financial-year-provisional-csv.csv'),
            ],
        );

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\StringEntry::class, $row->get('row'));
            });
            $total += $rows->count();
        }

        $this->assertSame(64892, $total);
    }
}
