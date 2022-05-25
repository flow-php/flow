<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Adapter\JSON\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\DSL\Json;
use Flow\ETL\Flow;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Stream\LocalFile;
use PHPUnit\Framework\TestCase;

final class JsonExtractorTest extends TestCase
{
    public function test_extracting_json_from_local_file_stream() : void
    {
        $rows = (new Flow())
            ->read(Json::from(new LocalFile(__DIR__ . '/../Fixtures/timezones.json'), 5))
            ->fetch();

        foreach ($rows as $row) {
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
        }

        $this->assertSame(247, $rows->count());
    }

    public function test_extracting_json_from_local_file_string_uri() : void
    {
        $extractor = new JsonExtractor(new LocalFile(__DIR__ . '/../Fixtures/timezones.json'), 5);

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
