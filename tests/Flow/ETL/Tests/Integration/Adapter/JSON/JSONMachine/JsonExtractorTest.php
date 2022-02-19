<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Adapter\JSON\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class JsonExtractorTest extends TestCase
{
    public function test_extracting_csv_files_with_header() : void
    {
        $extractor = new JsonExtractor(__DIR__ . '/../Fixtures/timezones.json', 5);

        $total = 0;
        /** @var Rows $rows */
        foreach ($extractor->extract() as $rows) {
            $rows->each(function (Row $row) : void {
                $this->assertInstanceOf(Row\Entry\ArrayEntry::class, $row->get('row'));
                $this->assertSame(
                    [
                        'timezones',
                        'latlng',
                        'name',
                        'country_code',
                        'capital',

                    ],
                    \array_keys($row->valueOf('row'))
                );
            });
            $total += $rows->count();
        }

        $this->assertSame(247, $total);
    }
}
