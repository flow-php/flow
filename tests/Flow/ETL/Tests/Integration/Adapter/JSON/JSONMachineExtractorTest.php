<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Adapter\JSON;

use Flow\ETL\Adapter\JSON\JSONMachineExtractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use JsonMachine\Items;
use PHPUnit\Framework\TestCase;

final class JSONMachineExtractorTest extends TestCase
{
    public function test_extracting_csv_files_with_header() : void
    {
        $jsonItems = Items::fromFile(__DIR__ . '/Fixtures/timezones.json');

        $extractor = new JSONMachineExtractor($jsonItems, 5);

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
